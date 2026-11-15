#!/usr/bin/env python3
"""端到端测试：收包去重、乱序撤销、重放切换、并发 CAS、崩溃恢复、回滚、已确认结果不可变。

运行：python3 -m unittest discover -s tests -v   （或 python3 tests/test_workbench.py）
"""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import threading
import unittest
import zipfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workbench.core import (Workbench, ConflictError, IngestError, NotFoundError)
from workbench import rules as rule_engine


def build_package(package_id, seq=1, details=(), corrections=()):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(
            {"package_id": package_id, "seq": seq, "source": "test"}))
        zf.writestr("details.jsonl", "\n".join(json.dumps(d) for d in details))
        if corrections:
            zf.writestr("corrections.jsonl", "\n".join(json.dumps(c) for c in corrections))
    return buf.getvalue()


def order(rid, date="2026-11-01", qty=1, price=10.0, currency="CNY", channel="web"):
    return {"record_id": rid, "order_date": date, "sku": "SKU-1", "qty": qty,
            "unit_price": price, "currency": currency, "channel": channel}


RULE_V2 = {
    "version": "v2",
    "key_field": "record_id",
    "transforms": [
        {"op": "rename", "from": "order_date", "to": "date"},
        {"op": "date_norm", "field": "date",
         "formats": ["%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"]},
        {"op": "round", "field": "unit_price", "digits": 2},
        {"op": "upper", "field": "currency"},
        {"op": "convert_currency", "field": "unit_price", "currency_field": "currency",
         "to": "CNY", "rates": {"CNY": 1.0, "USD": 7.2, "EUR": 7.8},
         "out_field": "unit_price_cny"},
        {"op": "compute", "out_field": "amount_cny",
         "expr": "qty*unit_price_cny", "round": 2},
    ],
    "filters": [{"field": "channel", "op": "eq", "value": "test"}],
}

RULE_V3 = {
    "version": "v3",
    "key_field": "record_id",
    "transforms": [{"op": "set", "field": "tag", "value": "v3"}],
    "filters": [],
}


class WorkbenchTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "test.db")
        self.wb = Workbench.open(self.db)

    def tearDown(self):
        self.wb.store.close()
        self.tmp.cleanup()

    # ---------------- 工具 ----------------

    def reopen(self):
        """模拟进程重启：关闭并以同一库文件重新打开（触发恢复流程）。"""
        self.wb.store.close()
        self.wb = Workbench.open(self.db)

    def sealed_rows(self):
        """所有已封存快照的行内容（用于不可变性断言）。"""
        return self.wb.store.q(
            "SELECT r.snapshot_id, r.record_id, r.content_hash, r.status, r.payload_json"
            " FROM rows r JOIN snapshots s ON s.id=r.snapshot_id"
            " WHERE s.status='sealed' ORDER BY r.snapshot_id, r.record_id")

    def current_rows(self):
        snap = self.wb.state()["current_snapshot"]
        return {r["record_id"]: r for r in self.wb.snapshot_rows(snap["id"], limit=10000)["rows"]}

    def register_v2(self):
        self.wb.register_rule(json.dumps(RULE_V2), note="升级：日期归一+币种折算+过滤test")

    # ---------------- 1. 收包：只收一次 ----------------

    def test_duplicate_package_ignored(self):
        blob = build_package("PKG-1", details=[order("rec-1")])
        r1 = self.wb.ingest_package(blob)
        self.assertEqual(r1["status"], "ok")
        r2 = self.wb.ingest_package(blob)
        self.assertEqual(r2["status"], "duplicate")
        self.assertEqual(self.wb.state()["counts"]["packages"], 1)
        kinds = [e["kind"] for e in self.wb.list_events()]
        self.assertIn("package_duplicate", kinds)

    def test_conflicting_package_id_rejected(self):
        self.wb.ingest_package(build_package("PKG-1", details=[order("rec-1")]))
        with self.assertRaises(ConflictError):
            self.wb.ingest_package(build_package("PKG-1", details=[order("rec-OTHER")]))
        self.assertEqual(self.wb.state()["counts"]["packages"], 1)

    def test_bad_package_rejected_atomically(self):
        bad = io.BytesIO()
        with zipfile.ZipFile(bad, "w") as zf:
            zf.writestr("manifest.json", json.dumps({"package_id": "PKG-BAD"}))
            zf.writestr("details.jsonl", '{"record_id": "rec-1"}\nNOT-JSON')
        with self.assertRaises(IngestError):
            self.wb.ingest_package(bad.getvalue())
        # 原子性：坏包不留下任何痕迹
        self.assertEqual(self.wb.state()["counts"]["packages"], 0)
        self.assertEqual(self.wb.state()["counts"]["raw_records"], 0)

    # ---------------- 2. 跨包更正与乱序撤销 ----------------

    def test_cross_package_correction(self):
        self.wb.ingest_package(build_package(
            "PKG-1", details=[order("rec-1", qty=1, price=100.0)]))
        self.wb.ingest_package(build_package(
            "PKG-2", corrections=[{"target": "rec-1", "mode": "merge",
                                   "fields": {"qty": 5}, "reason": "改数量"}]))
        rows = self.current_rows()
        self.assertEqual(json.loads(rows["rec-1"]["payload_json"])["qty"], 5)
        trace = self.wb.trace_record("rec-1")
        self.assertEqual({e["package_id"] for e in trace["events"]}, {"PKG-1", "PKG-2"})

    def test_revocation_before_original(self):
        # 撤销先于原件到达 → 悬挂
        self.wb.ingest_package(build_package(
            "PKG-1", corrections=[{"target": "rec-7", "mode": "revoke",
                                   "reason": "提前撤销"}]))
        self.assertEqual(self.wb.state()["counts"]["raw_pending"], 1)
        # 原件后到 → 出生即撤销，悬挂解决
        r = self.wb.ingest_package(build_package("PKG-2", details=[order("rec-7")]))
        self.assertEqual(r["pending_resolved"], 1)
        self.assertEqual(self.wb.state()["counts"]["raw_pending"], 0)
        rows = self.current_rows()
        self.assertEqual(rows["rec-7"]["status"], "revoked")
        trace = self.wb.trace_record("rec-7")
        self.assertEqual([e["kind"] for e in trace["events"]], ["revocation", "detail"])

    def test_correction_after_revocation_ignored_with_audit(self):
        self.wb.ingest_package(build_package("PKG-1", details=[order("rec-1")]))
        self.wb.ingest_package(build_package(
            "PKG-2", corrections=[{"target": "rec-1", "mode": "revoke", "reason": "x"}]))
        self.wb.ingest_package(build_package(
            "PKG-3", corrections=[{"target": "rec-1", "mode": "merge",
                                   "fields": {"qty": 99}, "reason": "太晚了"}]))
        rows = self.current_rows()
        self.assertEqual(rows["rec-1"]["status"], "revoked")
        self.assertNotEqual(json.loads(rows["rec-1"]["payload_json"] or "{}").get("qty"), 99)
        kinds = [e["kind"] for e in self.wb.list_events()]
        self.assertIn("ingest_warning", kinds)

    # ---------------- 3. 重放 + 新包到达 + 整体切换 ----------------

    def _seed_and_register_v2(self):
        self.wb.ingest_package(build_package("PKG-1", details=[
            order("rec-1", date="01/11/2026", qty=2, price=50.0, currency="USD"),
            order("rec-2", date="2026-11-02", qty=1, price=10.0, currency="CNY"),
            order("rec-3", date="20261103", qty=3, price=7.0, currency="EUR",
                  channel="test"),
        ]))
        self.register_v2()

    def test_replay_diff_publish_catchup(self):
        self._seed_and_register_v2()
        run_id = self.wb.start_replay("v2", background=False)
        # 重放（高水位冻结）之后到达的新包 —— 等价于"重放期间到达"
        self.wb.ingest_package(build_package("PKG-2", details=[
            order("rec-4", date="2026-11-05", qty=1, price=5.0, currency="CNY")]))

        diff = self.wb.diff_replay(run_id)
        # rec-1 的日期/币种被 v2 归一 → changed；rec-3 被 v2 过滤 → changed（active→filtered）
        changed_ids = {c["record_id"] for c in diff["changed"]}
        self.assertIn("rec-1", changed_ids)
        self.assertIn("rec-3", changed_ids)
        self.assertEqual(diff["uncovered_records"], 1)  # rec-4 在重放覆盖之外

        preview = self.wb.preview_switch(run_id)
        self.assertEqual(preview["catch_up_records"], 1)
        # rec-4 已用 v1 进入当前快照，切换后以 v2 呈现 → 记为变更而非新增
        self.assertEqual(preview["summary"]["added"], 0)
        self.assertIn("rec-4", {c["record_id"] for c in preview["changed"]})

        cur_id = self.wb.state()["current_snapshot"]["id"]
        result = self.wb.publish(run_id, expected_snapshot_id=cur_id,
                                 expected_high_water=preview["high_water_now"])
        new_snap = result["snapshot_id"]
        state = self.wb.state()
        self.assertEqual(state["current_snapshot"]["id"], new_snap)
        self.assertEqual(state["current_snapshot"]["rule_version"], "v2")

        # 不变式：新快照 100% 单一规则、覆盖全部已收包（含重放期间到达的 PKG-2）
        self.assertEqual(state["current_snapshot"]["coverage"], 2)
        rows = self.current_rows()
        self.assertEqual(set(rows), {"rec-1", "rec-2", "rec-3", "rec-4"})
        self.assertEqual(rows["rec-3"]["status"], "filtered")
        payload1 = json.loads(rows["rec-1"]["payload_json"])
        self.assertEqual(payload1["date"], "2026-11-01")  # 日期已归一
        self.assertEqual(payload1["amount_cny"], 2 * 50.0 * 7.2)

        # 每条结果都能追到原包与规则
        pkg_rows = self.wb.get_package("PKG-2")["current_rows"]
        self.assertEqual({r["record_id"] for r in pkg_rows}, {"rec-4"})
        trace = self.wb.trace_record("rec-4")
        self.assertEqual(trace["snapshot_rows"][-1]["rule_version"], "v2")

    def test_publish_cas_rejects_stale_pointer(self):
        """两个重放同时完成：只有一个能切换，另一个 409，需重新预检。"""
        self._seed_and_register_v2()
        self.wb.register_rule(json.dumps(RULE_V3), note="v3")
        run_a = self.wb.start_replay("v2", background=False)
        run_b = self.wb.start_replay("v3", background=False)
        cur_id = self.wb.state()["current_snapshot"]["id"]

        # 两个线程"同时"发布；结果必须是恰好一个成功
        outcomes = {}

        def do_publish(run, key):
            try:
                self.wb.publish(run, expected_snapshot_id=cur_id)
                outcomes[key] = "ok"
            except ConflictError:
                outcomes[key] = "409"

        t1 = threading.Thread(target=do_publish, args=(run_a, "a"))
        t2 = threading.Thread(target=do_publish, args=(run_b, "b"))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(sorted(outcomes.values()), ["409", "ok"])

        # 失败方重新预检后可再发布（基于新的当前快照）
        loser = run_a if outcomes["a"] == "409" else run_b
        preview = self.wb.preview_switch(loser)
        r = self.wb.publish(loser, expected_snapshot_id=preview["current_snapshot_id"],
                            expected_high_water=preview["high_water_now"])
        self.assertTrue(r["snapshot_id"] > 0)

    def test_publish_requires_completed_run(self):
        self._seed_and_register_v2()
        run_id = self.wb.start_replay("v2", background=False)
        cur = self.wb.state()["current_snapshot"]["id"]
        self.wb.publish(run_id, expected_snapshot_id=cur)
        # 已消费的重放不能再次发布
        with self.assertRaises(ConflictError):
            self.wb.publish(run_id, expected_snapshot_id=cur)

    def test_publish_high_water_guard(self):
        self._seed_and_register_v2()
        run_id = self.wb.start_replay("v2", background=False)
        preview = self.wb.preview_switch(run_id)
        # 预检后又来新包 → 高水位不一致 → 拒绝，需重新预检
        self.wb.ingest_package(build_package("PKG-2", details=[order("rec-9")]))
        with self.assertRaises(ConflictError):
            self.wb.publish(run_id,
                            expected_snapshot_id=preview["current_snapshot_id"],
                            expected_high_water=preview["high_water_now"])
        # 不带高水位校验则可以发布（调用方自行承担）
        self.wb.publish(run_id, expected_snapshot_id=preview["current_snapshot_id"])

    # ---------------- 4. 崩溃恢复 ----------------

    def test_recovery_marks_interrupted_and_preserves_confirmed(self):
        self._seed_and_register_v2()
        run_id = self.wb.start_replay("v2", background=False)
        cur = self.wb.state()["current_snapshot"]["id"]
        self.wb.publish(run_id, expected_snapshot_id=cur)
        sealed_before = self.sealed_rows()
        self.assertTrue(sealed_before)

        # 模拟崩溃：留一个 running 重放（带部分行），直接关掉
        with self.wb.store.write() as c:
            c.execute(
                "INSERT INTO replay_runs(rule_version, status, high_water, coverage_json,"
                " total, done, started_at) VALUES('v2','running',0,'[]',10,3,'t')")
            crash_run = c.execute("SELECT MAX(id) i FROM replay_runs").fetchone()["i"]
        self.reopen()  # 恢复流程

        runs = {r["id"]: r for r in self.wb.state()["replays"]}
        self.assertEqual(runs[crash_run]["status"], "interrupted")
        # 已确认结果未被恢复流程改动
        self.assertEqual(self.sealed_rows(), sealed_before)
        kinds = [e["kind"] for e in self.wb.list_events()]
        self.assertIn("replay_interrupted", kinds)
        self.assertIn("recovery_scan", kinds)

        # 中断的重放可以续跑到完成
        self.wb.resume_replay(crash_run, background=False)
        self.assertEqual(runs[crash_run]["status"], "interrupted")  # 旧快照
        runs2 = {r["id"]: r for r in self.wb.state()["replays"]}
        self.assertEqual(runs2[crash_run]["status"], "completed")
        self.assertEqual(self.sealed_rows(), sealed_before)  # 仍未动

    def test_recovery_does_not_move_pointer(self):
        self._seed_and_register_v2()
        before = self.wb.state()["current_snapshot"]["id"]
        self.reopen()
        self.assertEqual(self.wb.state()["current_snapshot"]["id"], before)

    # ---------------- 5. 回滚 ----------------

    def test_rollback_restores_old_rule_on_current_data(self):
        self._seed_and_register_v2()
        run_id = self.wb.start_replay("v2", background=False)
        v1_snap = self.wb.state()["current_snapshot"]["id"]
        self.wb.publish(run_id, expected_snapshot_id=v1_snap)
        self.assertEqual(self.wb.state()["current_snapshot"]["rule_version"], "v2")
        sealed_after_publish = self.sealed_rows()

        # 升级失败 → 回滚到 v1 快照
        r = self.wb.rollback(to_snapshot_id=v1_snap,
                             expected_snapshot_id=self.wb.state()["current_snapshot"]["id"],
                             reason="v2 汇率表有误")
        self.assertEqual(r["rule_version"], "v1")
        state = self.wb.state()
        self.assertEqual(state["current_snapshot"]["rule_version"], "v1")
        # 回滚是新快照，旧快照原样保留（append-only 历史）
        self.assertEqual(self.sealed_rows()[:len(sealed_after_publish)], sealed_after_publish)
        snaps = {s["id"]: s for s in state["snapshots"]}
        self.assertEqual(snaps[v1_snap]["status"], "sealed")
        # 回滚后的行 = v1 规则对当前原始数据的重算（rec-3 不再被过滤）
        rows = self.current_rows()
        self.assertEqual(rows["rec-3"]["status"], "active")

    def test_rollback_requires_reason_and_cas(self):
        self._seed_and_register_v2()
        run_id = self.wb.start_replay("v2", background=False)
        v1_snap = self.wb.state()["current_snapshot"]["id"]
        self.wb.publish(run_id, expected_snapshot_id=v1_snap)
        with self.assertRaises(IngestError):
            self.wb.rollback(to_snapshot_id=v1_snap,
                             expected_snapshot_id=self.wb.state()["current_snapshot"]["id"],
                             reason="")
        with self.assertRaises(ConflictError):
            self.wb.rollback(to_snapshot_id=v1_snap,
                             expected_snapshot_id=9999, reason="x")

    # ---------------- 6. 已确认结果在任何操作下不被静默修改 ----------------

    def test_confirmed_results_immutable_across_operations(self):
        self._seed_and_register_v2()
        run_id = self.wb.start_replay("v2", background=False)
        v1_snap = self.wb.state()["current_snapshot"]["id"]
        self.wb.publish(run_id, expected_snapshot_id=v1_snap)
        sealed = self.sealed_rows()
        sealed_snap_ids = {r["snapshot_id"] for r in sealed}

        # 后续所有操作：新包、更正、重放、中断、恢复、回滚 —— 已封存行不得变化
        self.wb.ingest_package(build_package("PKG-2", details=[order("rec-4")]))
        self.wb.ingest_package(build_package(
            "PKG-3", corrections=[{"target": "rec-1", "mode": "merge",
                                   "fields": {"qty": 99}, "reason": "更正"}]))
        self.wb.start_replay("v1", background=False)
        self.reopen()
        self.wb.rollback(to_snapshot_id=v1_snap,
                         expected_snapshot_id=self.wb.state()["current_snapshot"]["id"],
                         reason="验证不可变性")
        # 原本已封存的快照行逐条保持不变（回滚新封存的快照不在比较范围）
        after = [r for r in self.sealed_rows() if r["snapshot_id"] in sealed_snap_ids]
        self.assertEqual(
            [(r["snapshot_id"], r["record_id"], r["content_hash"], r["status"],
              r["payload_json"]) for r in after],
            [(r["snapshot_id"], r["record_id"], r["content_hash"], r["status"],
              r["payload_json"]) for r in sealed])

    # ---------------- 7. 规则引擎 ----------------

    def test_rule_engine_transforms_and_safe_eval(self):
        spec = rule_engine.validate_spec(RULE_V2)
        out = rule_engine.apply_rule(spec, "rec-1",
                                     {"order_date": "02/11/2026", "qty": 2,
                                      "unit_price": 10.0, "currency": "usd",
                                      "channel": "web"}, "active")
        self.assertEqual(out["payload"]["date"], "2026-11-02")
        self.assertEqual(out["payload"]["currency"], "USD")
        self.assertEqual(out["payload"]["amount_cny"], 144.0)
        with self.assertRaises(rule_engine.RuleError):
            rule_engine.validate_spec({**RULE_V2, "transforms": [
                {"op": "compute", "out_field": "x",
                 "expr": "__import__('os').system('id')"}]})

    def test_register_duplicate_rule_rejected(self):
        self.register_v2()
        with self.assertRaises(ConflictError):
            self.register_v2()
        with self.assertRaises(rule_engine.RuleError):
            self.wb.register_rule("{not json")


class HttpSmokeTest(unittest.TestCase):
    """HTTP 层冒烟：真实起服务、真实 POST 包、真实拉页面。"""

    def test_server_end_to_end(self):
        import time
        import urllib.request
        from workbench.server import serve

        with tempfile.TemporaryDirectory() as tmp:
            httpd = serve(os.path.join(tmp, "http.db"), "127.0.0.1", 0)
            port = httpd.server_address[1]
            t = threading.Thread(target=httpd.serve_forever, daemon=True)
            t.start()
            try:
                base = f"http://127.0.0.1:{port}"
                blob = build_package("PKG-HTTP", details=[order("rec-1")])
                req = urllib.request.Request(base + "/api/packages", data=blob,
                                             method="POST")
                with urllib.request.urlopen(req, timeout=5) as resp:
                    out = json.loads(resp.read())
                self.assertEqual(out["status"], "ok")

                with urllib.request.urlopen(base + "/api/state", timeout=5) as resp:
                    state = json.loads(resp.read())
                self.assertEqual(state["counts"]["packages"], 1)

                with urllib.request.urlopen(base + "/", timeout=5) as resp:
                    html = resp.read().decode()
                self.assertIn("接收工作台", html)

                # 重发 → duplicate
                req = urllib.request.Request(base + "/api/packages", data=blob,
                                             method="POST")
                with urllib.request.urlopen(req, timeout=5) as resp:
                    out = json.loads(resp.read())
                self.assertEqual(out["status"], "duplicate")
            finally:
                httpd.shutdown()
                httpd.server_close()


if __name__ == "__main__":
    unittest.main()
