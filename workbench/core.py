"""核心逻辑：收包（一次）、原始层事件、影子重放、差异核对、原子切换、回滚、恢复。

不变式：
1. 原始包只收一次：sha256 去重；package_id 冲突（同号不同内容）拒绝。
2. 任一时刻恰好一个 open 快照；sealed 快照及其行永不修改。
3. 发布的快照必然覆盖全部已收包、且 100% 单一规则版本 —— 重放期间到达的包
   在切换事务内用新规则追平（catch-up），绝不混入旧规则的半成品。
4. 发布/回滚均为 CAS：expected_snapshot_id（+ 可选 expected_high_water）不匹配则 409，
   两个重放同时完成时只有一个能切换，另一个必须重新预检。
5. 恢复流程只把 running 重放标记为 interrupted，不触碰已确认（sealed）结果。
"""

from __future__ import annotations

import hashlib
import io
import json
import threading
import zipfile
from datetime import datetime, timezone

from .db import Store
from . import rules as rule_engine

MAX_PACKAGE_BYTES = 64 * 1024 * 1024
REPLAY_CHUNK = 200

DEFAULT_RULE_V1 = {
    "version": "v1",
    "key_field": "record_id",
    "transforms": [
        {"op": "rename", "from": "order_date", "to": "date"},
        {"op": "round", "field": "unit_price", "digits": 2},
        {"op": "upper", "field": "currency"},
    ],
    "filters": [],
}


class IngestError(Exception):
    """请求数据有问题（HTTP 400）。"""


class NotFoundError(Exception):
    """对象不存在（HTTP 404）。"""


class ConflictError(Exception):
    """状态冲突（HTTP 409）：重复包号、CAS 失败、重放状态不允许等。"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _j(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


# ---------------------------------------------------------------- 原始层状态折叠

def _empty_state():
    return {"records": {}, "pending": {}}  # records: rid -> dict；pending: rid -> [event,...]


def _apply_event(state, event, warnings):
    """把一条原始事件应用到内存状态。event: {id, kind, record_id, payload}。

    语义（与规则无关，属于源数据事实）：
    - detail：新建记录；若该记录有悬挂更正/撤销，按事件序立即解决（原件晚到 ⇒ 出生即撤销）。
    - correction：merge（默认）字段合并 / replace 整记录替换；目标不存在 ⇒ 悬挂。
    - revocation：置为 revoked；目标不存在 ⇒ 悬挂。
    - 重复 detail / 对已撤销记录的更正：不静默，产出 warning 交给审计。
    """
    records, pending = state["records"], state["pending"]
    kind, rid, payload = event["kind"], event["record_id"], event["payload"]

    if kind == "detail":
        if rid in records:
            warnings.append(f"记录 {rid} 重复明细（事件 {event['id']}），保留首份")
            return
        records[rid] = {
            "fields": dict(payload),
            "status": "active",
            "origin_package_id": event["package_id"],
            "last_event_id": event["id"],
        }
        for p in pending.pop(rid, []):  # 悬挂事件按到达顺序解决
            _apply_correction(records, p, warnings)
        return

    # correction / revocation
    if rid in records:
        _apply_correction(records, event, warnings)
    else:
        pending.setdefault(rid, []).append(event)


def _apply_correction(records, event, warnings):
    rid = event["record_id"]
    rec = records[rid]
    payload = event["payload"]
    # 悬挂事件在原件到达时补应用，其 id 早于明细 id；取 max 保持单调，
    # 使 last_event_id 恒等于该记录已应用事件的最大 id。
    rec["last_event_id"] = max(rec["last_event_id"], event["id"])
    if event["kind"] == "revocation":
        if rec["status"] == "revoked":
            warnings.append(f"记录 {rid} 重复撤销（事件 {event['id']}），忽略")
            return
        rec["status"] = "revoked"
        return
    # correction
    if rec["status"] == "revoked":
        warnings.append(f"记录 {rid} 已撤销，忽略更正（事件 {event['id']}）")
        return
    mode = payload.get("mode", "merge")
    fields = payload.get("fields", {})
    if mode == "replace":
        new_fields = dict(fields)
        new_fields.setdefault("record_id", rid)
        rec["fields"] = new_fields
    else:
        rec["fields"].update(fields)


def fold_events(conn, high_water: int) -> dict:
    """把 raw_events 折叠到指定高水位，重建该时点的原始层状态。"""
    state = _empty_state()
    warnings = []
    cur = conn.execute(
        "SELECT id, package_id, kind, record_id, payload_json FROM raw_events "
        "WHERE id <= ? ORDER BY id", (high_water,))
    for row in cur:
        _apply_event(state, {
            "id": row["id"], "package_id": row["package_id"], "kind": row["kind"],
            "record_id": row["record_id"], "payload": json.loads(row["payload_json"]),
        }, warnings)
    return state


# ---------------------------------------------------------------- 工作台

class Workbench:
    def __init__(self, store: Store):
        self.store = store
        self._cancel: dict[int, threading.Event] = {}
        self._threads: dict[int, threading.Thread] = {}

    # ---------------- 构造/引导/恢复 ----------------

    @classmethod
    def open(cls, db_path: str) -> "Workbench":
        store = Store(db_path)
        wb = cls(store)
        with store.write() as c:
            if not c.execute("SELECT 1 FROM rule_versions LIMIT 1").fetchone():
                c.execute(
                    "INSERT INTO rule_versions(version, spec_json, note, created_at) VALUES(?,?,?,?)",
                    (DEFAULT_RULE_V1["version"], _j(DEFAULT_RULE_V1),
                     "内置初版规则", _now()))
                c.execute(
                    "INSERT INTO events(ts, kind, actor, summary, detail_json) VALUES(?,?,?,?,?)",
                    (_now(), "rule_registered", "system", "注册内置规则 v1", None))
            if not c.execute("SELECT 1 FROM snapshots LIMIT 1").fetchone():
                cur = c.execute(
                    "INSERT INTO snapshots(rule_version, status, origin, coverage, high_water, created_at)"
                    " VALUES(?,?,?,?,?,?)",
                    (DEFAULT_RULE_V1["version"], "open", "init", 0, 0, _now()))
                sid = cur.lastrowid
                c.execute("INSERT INTO pointer(id, snapshot_id) VALUES(1, ?)", (sid,))
        wb.recover()
        return wb

    def recover(self):
        """启动恢复：只处理'运行中'的半成品，绝不修改 sealed 快照与指针所指的已确认结果。"""
        with self.store.write() as c:
            interrupted = c.execute(
                "SELECT id, rule_version FROM replay_runs WHERE status='running'").fetchall()
            for r in interrupted:
                c.execute("UPDATE replay_runs SET status='interrupted', finished_at=? WHERE id=?",
                          (_now(), r["id"]))
                self._event(c, "replay_interrupted", "system",
                            f"恢复：重放 #{r['id']}（{r['rule_version']}）在进程中断时未完成，标记为 interrupted",
                            {"run_id": r["id"]})
            opens = c.execute("SELECT id FROM snapshots WHERE status='open'").fetchall()
            ptr = c.execute("SELECT snapshot_id FROM pointer WHERE id=1").fetchone()
            if len(opens) == 0:
                # 防御性修复：正常不会发生。新建 open 快照而不是修改任何 sealed 行。
                latest = c.execute(
                    "SELECT rule_version FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()
                rule = latest["rule_version"] if latest else DEFAULT_RULE_V1["version"]
                sid = self._create_snapshot_from_rule(c, rule, "recovery:rebuild")
                c.execute("UPDATE pointer SET snapshot_id=? WHERE id=1", (sid,))
                self._event(c, "recovery_rebuilt", "system",
                            f"恢复：未发现开放快照，按规则 {rule} 重建 #{sid}", None)
            elif ptr is None or ptr["snapshot_id"] not in [o["id"] for o in opens]:
                c.execute("UPDATE pointer SET snapshot_id=? WHERE id=1", (opens[0]["id"],))
                self._event(c, "recovery_pointer", "system",
                            f"恢复：指针修正到开放快照 #{opens[0]['id']}", None)
            n_pkg = c.execute("SELECT COUNT(*) n FROM packages").fetchone()["n"]
            n_rows = c.execute("SELECT COUNT(*) n FROM raw_events").fetchone()["n"]
            self._event(c, "recovery_scan", "system",
                        f"启动恢复扫描：包 {n_pkg}，原始事件 {n_rows}，"
                        f"中断重放 {len(interrupted)}", None)

    # ---------------- 工具 ----------------

    def _event(self, c, kind, actor, summary, detail):
        c.execute(
            "INSERT INTO events(ts, kind, actor, summary, detail_json) VALUES(?,?,?,?,?)",
            (_now(), kind, actor, summary, _j(detail) if detail is not None else None))

    def _current_snapshot(self, c):
        row = c.execute(
            "SELECT s.* FROM snapshots s JOIN pointer p ON p.snapshot_id=s.id WHERE p.id=1"
        ).fetchone()
        if row is None:
            raise ConflictError("没有当前快照（存储损坏）")
        return row

    def _high_water(self, c) -> int:
        return c.execute("SELECT COALESCE(MAX(id),0) h FROM raw_events").fetchone()["h"]

    def _rule_spec(self, c, version: str) -> dict:
        row = c.execute("SELECT spec_json FROM rule_versions WHERE version=?",
                        (version,)).fetchone()
        if row is None:
            raise NotFoundError(f"规则版本不存在: {version}")
        return json.loads(row["spec_json"])

    def _derive_row(self, spec, rid, rec):
        out = rule_engine.apply_rule(spec, rid, rec["fields"], rec["status"])
        return {
            "record_id": rid,
            "package_id": rec["origin_package_id"],
            "record_key": out["record_key"],
            "status": out["status"],
            "payload_json": _j(out["payload"]) if out["payload"] is not None else None,
            "content_hash": rule_engine.content_hash_of(out["status"], out["payload"]),
        }

    # ---------------- 收包 ----------------

    def ingest_package(self, blob: bytes, source: str = "sender", actor: str = "sender") -> dict:
        if len(blob) > MAX_PACKAGE_BYTES:
            raise IngestError("包超过大小上限")
        digest = hashlib.sha256(blob).hexdigest()
        manifest, details, corrections = self._parse_zip(blob)

        with self.store.write() as c:
            dup = c.execute("SELECT package_id FROM packages WHERE sha256=?",
                            (digest,)).fetchone()
            if dup:
                self._event(c, "package_duplicate", actor,
                            f"重复投递 {dup['package_id']}（内容一致），已忽略",
                            {"package_id": dup["package_id"], "sha256": digest})
                return {"status": "duplicate", "package_id": dup["package_id"]}

            package_id = manifest["package_id"]
            if c.execute("SELECT 1 FROM packages WHERE package_id=?",
                         (package_id,)).fetchone():
                self._event(c, "package_conflict", actor,
                            f"包号 {package_id} 已存在但内容不同，拒绝接收",
                            {"package_id": package_id, "sha256": digest})
                raise ConflictError(f"包号 {package_id} 已存在但内容不同")

            c.execute(
                "INSERT INTO packages(package_id, sha256, source, seq, received_at, size,"
                " n_details, n_corrections, status, raw, manifest_json)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (package_id, digest, source, manifest.get("seq"), _now(), len(blob),
                 len(details), len(corrections), "received", blob, _j(manifest)))

            # 1) 事件落日志（append-only）
            new_events = []
            for d in details:
                rid = d.get("record_id")
                if not rid:
                    raise IngestError("明细缺少 record_id")
                fields = {k: v for k, v in d.items()}
                cur = c.execute(
                    "INSERT INTO raw_events(package_id, kind, record_id, payload_json, created_at)"
                    " VALUES(?,?,?,?,?)",
                    (package_id, "detail", rid, _j(fields), _now()))
                new_events.append({"id": cur.lastrowid, "package_id": package_id,
                                   "kind": "detail", "record_id": rid, "payload": fields})
            for corr in corrections:
                target = corr.get("target")
                if not target:
                    raise IngestError("更正记录缺少 target")
                mode = corr.get("mode", "merge")
                if mode not in ("merge", "replace", "revoke"):
                    raise IngestError(f"未知更正模式: {mode}")
                kind = "revocation" if mode == "revoke" else "correction"
                payload = {"mode": mode, "fields": corr.get("fields", {}),
                           "reason": corr.get("reason", "")}
                cur = c.execute(
                    "INSERT INTO raw_events(package_id, kind, record_id, payload_json, created_at)"
                    " VALUES(?,?,?,?,?)",
                    (package_id, kind, target, _j(payload), _now()))
                new_events.append({"id": cur.lastrowid, "package_id": package_id,
                                   "kind": kind, "record_id": target, "payload": payload})

            # 2) 载入受影响记录的当前状态与悬挂项，在内存中按统一语义应用
            affected = sorted({e["record_id"] for e in new_events})
            state = _empty_state()
            if affected:
                marks = ",".join("?" * len(affected))
                for r in c.execute(
                        f"SELECT * FROM raw_records WHERE record_id IN ({marks})", affected):
                    state["records"][r["record_id"]] = {
                        "fields": json.loads(r["fields_json"]),
                        "status": r["status"],
                        "origin_package_id": r["origin_package_id"],
                        "last_event_id": r["last_event_id"],
                    }
                for r in c.execute(
                        f"SELECT event_id, target_record_id, kind, payload_json FROM raw_pending"
                        f" WHERE target_record_id IN ({marks}) ORDER BY event_id", affected):
                    ev = c.execute("SELECT package_id FROM raw_events WHERE id=?",
                                   (r["event_id"],)).fetchone()
                    state["pending"].setdefault(r["target_record_id"], []).append({
                        "id": r["event_id"], "package_id": ev["package_id"],
                        "kind": r["kind"], "record_id": r["target_record_id"],
                        "payload": json.loads(r["payload_json"]),
                    })

            warnings: list[str] = []
            for e in new_events:
                _apply_event(state, e, warnings)

            # 3) 写回：记录状态、悬挂新增/解决。
            # _apply_event 已在 detail 到达时把对应悬挂项弹出并应用，
            # 因此 state["pending"] 中剩下的都是目标仍未到的悬挂。
            if affected:
                solved = [rid for rid in affected if rid in state["records"]]
                if solved:
                    m2 = ",".join("?" * len(solved))
                    cur = c.execute(
                        f"DELETE FROM raw_pending WHERE target_record_id IN ({m2})", solved)
                    n_resolved = cur.rowcount
                else:
                    n_resolved = 0
            else:
                n_resolved = 0
            # 新增悬挂：本批事件中目标仍不存在的更正/撤销
            n_pending_added = 0
            known_pending_event_ids = set()
            for r in c.execute(
                    "SELECT event_id, target_record_id FROM raw_pending"
                    + (f" WHERE target_record_id IN ({','.join('?'*len(affected))})" if affected else ""),
                    affected):
                known_pending_event_ids.add(r["event_id"])
            for rid, evs in state["pending"].items():
                for e in evs:
                    if e["id"] in known_pending_event_ids:
                        continue
                    c.execute(
                        "INSERT INTO raw_pending(event_id, target_record_id, kind, payload_json, created_at)"
                        " VALUES(?,?,?,?,?)",
                        (e["id"], rid, e["kind"], _j(e["payload"]), _now()))
                    n_pending_added += 1
                    self._event(c, "correction_pending", actor,
                                f"更正/撤销目标 {rid} 尚未到达，悬挂等待（事件 {e['id']}）",
                                {"target": rid, "event_id": e["id"], "kind": e["kind"]})

            changed_records = []
            for rid, rec in state["records"].items():
                c.execute(
                    "INSERT INTO raw_records(record_id, fields_json, status, origin_package_id,"
                    " last_event_id, updated_at) VALUES(?,?,?,?,?,?)"
                    " ON CONFLICT(record_id) DO UPDATE SET fields_json=excluded.fields_json,"
                    " status=excluded.status, last_event_id=excluded.last_event_id,"
                    " updated_at=excluded.updated_at",
                    (rid, _j(rec["fields"]), rec["status"], rec["origin_package_id"],
                     rec["last_event_id"], _now()))
                changed_records.append((rid, rec))

            # 4) 现行规则派生：只更新当前开放快照中受影响的行
            snap = self._current_snapshot(c)
            spec = self._rule_spec(c, snap["rule_version"])
            for rid, rec in changed_records:
                row = self._derive_row(spec, rid, rec)
                c.execute(
                    "INSERT INTO rows(snapshot_id, record_id, package_id, record_key, status,"
                    " payload_json, content_hash) VALUES(?,?,?,?,?,?,?)"
                    " ON CONFLICT(snapshot_id, record_id) DO UPDATE SET"
                    " record_key=excluded.record_key, status=excluded.status,"
                    " payload_json=excluded.payload_json, content_hash=excluded.content_hash",
                    (snap["id"], rid, row["package_id"], row["record_key"], row["status"],
                     row["payload_json"], row["content_hash"]))

            c.execute("UPDATE packages SET status='parsed' WHERE package_id=?", (package_id,))
            for w in warnings:
                self._event(c, "ingest_warning", actor, f"包 {package_id}: {w}",
                            {"package_id": package_id})
            self._event(c, "package_received", actor,
                        f"接收包 {package_id}：明细 {len(details)}，更正 {len(corrections)}，"
                        f"影响记录 {len(changed_records)}，新悬挂 {n_pending_added}，"
                        f"解决悬挂 {n_resolved}",
                        {"package_id": package_id, "sha256": digest,
                         "snapshot_id": snap["id"]})
            return {"status": "ok", "package_id": package_id,
                    "records_affected": len(changed_records),
                    "pending_added": n_pending_added, "pending_resolved": n_resolved,
                    "warnings": warnings}

    @staticmethod
    def _parse_zip(blob: bytes):
        try:
            zf = zipfile.ZipFile(io.BytesIO(blob))
        except zipfile.BadZipFile as e:
            raise IngestError(f"不是合法 zip 包: {e}")
        names = set(zf.namelist())
        if "manifest.json" not in names:
            raise IngestError("包内缺少 manifest.json")
        try:
            manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise IngestError(f"manifest.json 解析失败: {e}")
        if not manifest.get("package_id"):
            raise IngestError("manifest 缺少 package_id")

        def read_jsonl(name, required):
            if name not in names:
                if required:
                    raise IngestError(f"包内缺少 {name}")
                return []
            out = []
            try:
                text = zf.read(name).decode("utf-8")
            except UnicodeDecodeError as e:
                raise IngestError(f"{name} 编码错误: {e}")
            for i, line in enumerate(text.splitlines(), 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError as e:
                    raise IngestError(f"{name} 第 {i} 行 JSON 错误: {e}")
            return out

        details = read_jsonl("details.jsonl", required=True)
        corrections = read_jsonl("corrections.jsonl", required=False)
        return manifest, details, corrections

    # ---------------- 规则 ----------------

    def register_rule(self, spec_text: str, note: str = "", actor: str = "operator") -> dict:
        spec = rule_engine.spec_from_json(spec_text)
        with self.store.write() as c:
            if c.execute("SELECT 1 FROM rule_versions WHERE version=?",
                         (spec["version"],)).fetchone():
                raise ConflictError(f"规则版本已存在: {spec['version']}")
            c.execute(
                "INSERT INTO rule_versions(version, spec_json, note, created_at) VALUES(?,?,?,?)",
                (spec["version"], _j(spec), note, _now()))
            self._event(c, "rule_registered", actor,
                        f"注册规则 {spec['version']}" + (f"：{note}" if note else ""),
                        {"version": spec["version"]})
        return {"version": spec["version"]}

    # ---------------- 重放 ----------------

    def start_replay(self, rule_version: str, actor: str = "operator",
                     background: bool = True) -> int:
        with self.store.write() as c:
            self._rule_spec(c, rule_version)  # 校验存在
            hw = self._high_water(c)
            coverage = [r["package_id"] for r in
                        c.execute("SELECT package_id FROM packages ORDER BY received_at")]
            cur = c.execute(
                "INSERT INTO replay_runs(rule_version, status, high_water, coverage_json,"
                " started_at) VALUES(?,?,?,?,?)",
                (rule_version, "running", hw, _j(coverage), _now()))
            run_id = cur.lastrowid
            self._event(c, "replay_started", actor,
                        f"开始重放 #{run_id}：规则 {rule_version}，高水位 {hw}，"
                        f"覆盖包 {len(coverage)} 个", {"run_id": run_id})
        if background:
            self._spawn_replay(run_id)
        else:
            self.run_replay_body(run_id)
        return run_id

    def _spawn_replay(self, run_id: int):
        cancel = threading.Event()
        self._cancel[run_id] = cancel
        t = threading.Thread(target=self._replay_thread, args=(run_id, cancel),
                             daemon=True, name=f"replay-{run_id}")
        self._threads[run_id] = t
        t.start()

    def _replay_thread(self, run_id: int, cancel: threading.Event):
        try:
            self.run_replay_body(run_id, cancel)
        except Exception as e:  # 重放异常不拖垮服务
            with self.store.write() as c:
                r = c.execute("SELECT status FROM replay_runs WHERE id=?", (run_id,)).fetchone()
                if r and r["status"] == "running":
                    c.execute("UPDATE replay_runs SET status='failed', error=?, finished_at=?"
                              " WHERE id=?", (str(e), _now(), run_id))
                    self._event(c, "replay_failed", "system",
                                f"重放 #{run_id} 失败：{e}", {"run_id": run_id})

    def run_replay_body(self, run_id: int, cancel: threading.Event | None = None):
        """重放主体：按启动时冻结的高水位折叠原始事件，用指定规则重算。可断点续跑。"""
        with self.store.read() as c:
            run = c.execute("SELECT * FROM replay_runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                raise NotFoundError(f"重放不存在: {run_id}")
            spec = self._rule_spec(c, run["rule_version"])
            state = fold_events(c, run["high_water"])
        records = state["records"]

        with self.store.write() as c:
            done_existing = c.execute(
                "SELECT COUNT(*) n FROM replay_rows WHERE run_id=?", (run_id,)).fetchone()["n"]
            c.execute("UPDATE replay_runs SET total=?, done=? WHERE id=?",
                      (len(records), done_existing, run_id))

        todo = [rid for rid in sorted(records.keys())]
        for i in range(0, len(todo), REPLAY_CHUNK):
            if cancel is not None and cancel.is_set():
                with self.store.write() as c:
                    r = c.execute("SELECT status FROM replay_runs WHERE id=?",
                                  (run_id,)).fetchone()
                    if r and r["status"] == "running":
                        c.execute("UPDATE replay_runs SET status='interrupted', finished_at=?"
                                  " WHERE id=?", (_now(), run_id))
                        self._event(c, "replay_interrupted", "operator",
                                    f"重放 #{run_id} 被手动中断", {"run_id": run_id})
                return
            chunk = todo[i:i + REPLAY_CHUNK]
            with self.store.write() as c:
                r = c.execute("SELECT status FROM replay_runs WHERE id=?",
                              (run_id,)).fetchone()
                if r is None or r["status"] != "running":
                    return  # 被外部中断/失败，静默退出（状态已由外部落库）
                for rid in chunk:
                    rec = records[rid]
                    row = self._derive_row(spec, rid, rec)
                    c.execute(
                        "INSERT INTO replay_rows(run_id, record_id, package_id, record_key,"
                        " status, payload_json, content_hash) VALUES(?,?,?,?,?,?,?)"
                        " ON CONFLICT(run_id, record_id) DO UPDATE SET"
                        " record_key=excluded.record_key, status=excluded.status,"
                        " payload_json=excluded.payload_json,"
                        " content_hash=excluded.content_hash",
                        (run_id, rid, row["package_id"], row["record_key"], row["status"],
                         row["payload_json"], row["content_hash"]))
                done = c.execute("SELECT COUNT(*) n FROM replay_rows WHERE run_id=?",
                                 (run_id,)).fetchone()["n"]
                c.execute("UPDATE replay_runs SET done=? WHERE id=?", (done, run_id))

        with self.store.write() as c:
            r = c.execute("SELECT status FROM replay_runs WHERE id=?", (run_id,)).fetchone()
            if r and r["status"] == "running":
                c.execute("UPDATE replay_runs SET status='completed', finished_at=? WHERE id=?",
                          (_now(), run_id))
                self._event(c, "replay_completed", "system",
                            f"重放 #{run_id} 完成：{len(records)} 条记录",
                            {"run_id": run_id})

    def interrupt_replay(self, run_id: int, actor: str = "operator"):
        ev = self._cancel.get(run_id)
        if ev is not None:
            ev.set()
        with self.store.write() as c:
            r = c.execute("SELECT status FROM replay_runs WHERE id=?", (run_id,)).fetchone()
            if r is None:
                raise NotFoundError(f"重放不存在: {run_id}")
            if r["status"] == "running":
                c.execute("UPDATE replay_runs SET status='interrupted', finished_at=? WHERE id=?",
                          (_now(), run_id))
                self._event(c, "replay_interrupted", actor,
                            f"重放 #{run_id} 被中断", {"run_id": run_id})

    def resume_replay(self, run_id: int, actor: str = "operator",
                      background: bool = True):
        with self.store.write() as c:
            r = c.execute("SELECT status FROM replay_runs WHERE id=?", (run_id,)).fetchone()
            if r is None:
                raise NotFoundError(f"重放不存在: {run_id}")
            if r["status"] != "interrupted":
                raise ConflictError(f"重放 #{run_id} 状态为 {r['status']}，不能续跑")
            c.execute("UPDATE replay_runs SET status='running', error=NULL, finished_at=NULL"
                      " WHERE id=?", (run_id,))
            self._event(c, "replay_resumed", actor, f"重放 #{run_id} 续跑",
                        {"run_id": run_id})
        if background:
            self._spawn_replay(run_id)
        else:
            self.run_replay_body(run_id)

    # ---------------- 差异 ----------------

    @staticmethod
    def _row_map(rows_iter):
        out = {}
        for r in rows_iter:
            out[r["record_id"]] = {
                "record_id": r["record_id"], "package_id": r["package_id"],
                "record_key": r["record_key"], "status": r["status"],
                "payload": json.loads(r["payload_json"]) if r["payload_json"] else None,
                "content_hash": r["content_hash"],
            }
        return out

    @staticmethod
    def _diff_maps(old: dict, new: dict) -> dict:
        added, removed, changed = [], [], []
        for rid in sorted(new.keys() - old.keys()):
            added.append(new[rid])
        for rid in sorted(old.keys() - new.keys()):
            removed.append(old[rid])
        for rid in sorted(old.keys() & new.keys()):
            o, n = old[rid], new[rid]
            if o["content_hash"] == n["content_hash"]:
                continue
            field_diff = {}
            keys = set((o["payload"] or {}).keys()) | set((n["payload"] or {}).keys())
            for k in sorted(keys):
                ov = (o["payload"] or {}).get(k)
                nv = (n["payload"] or {}).get(k)
                if ov != nv:
                    field_diff[k] = [ov, nv]
            changed.append({"record_id": rid, "package_id": n["package_id"],
                            "old_status": o["status"], "new_status": n["status"],
                            "fields": field_diff})
        return {"added": added, "removed": removed, "changed": changed,
                "summary": {"added": len(added), "removed": len(removed),
                            "changed": len(changed)}}

    def diff_replay(self, run_id: int) -> dict:
        """重放结果 vs 当前快照（限定在重放覆盖的原始状态范围内，保证可比）。"""
        with self.store.read() as c:
            run = c.execute("SELECT * FROM replay_runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                raise NotFoundError(f"重放不存在: {run_id}")
            if run["status"] not in ("completed", "consumed"):
                raise ConflictError(f"重放 #{run_id} 尚未完成（{run['status']}）")
            snap = self._current_snapshot(c)
            hw = run["high_water"]
            # 两侧都限定"高水位前到达且此后未变更"的记录，保证 diff 只反映规则差异；
            # 重放期间新到/变更的记录单独计数，由切换时的追平处理。
            cur_rows = c.execute(
                "SELECT r.* FROM rows r JOIN raw_records rr ON rr.record_id=r.record_id"
                " WHERE r.snapshot_id=? AND rr.last_event_id<=?", (snap["id"], hw)).fetchall()
            new_rows = c.execute(
                "SELECT rp.* FROM replay_rows rp JOIN raw_records rr"
                " ON rr.record_id=rp.record_id"
                " WHERE rp.run_id=? AND rr.last_event_id<=?", (run_id, hw)).fetchall()
            uncovered = c.execute(
                "SELECT COUNT(*) n FROM raw_records WHERE last_event_id>?", (hw,)).fetchone()["n"]
            diff = self._diff_maps(self._row_map(cur_rows), self._row_map(new_rows))
            diff.update({
                "run_id": run_id, "rule_version": run["rule_version"],
                "current_snapshot_id": snap["id"],
                "current_rule": snap["rule_version"],
                "high_water": hw,
                "uncovered_records": uncovered,
            })
            return diff

    # ---------------- 候选集构建 / 预检 / 发布 ----------------

    def _build_candidate(self, c, run) -> dict:
        """构建'若现在切换'的完整候选行集：重放覆盖部分取重放行，
        重放期间新到/变更的记录用新规则现场追平 —— 保证候选集 100% 单一规则。"""
        spec = json.loads(c.execute("SELECT spec_json FROM rule_versions WHERE version=?",
                                    (run["rule_version"],)).fetchone()["spec_json"])
        replay_rows = {r["record_id"]: r for r in c.execute(
            "SELECT * FROM replay_rows WHERE run_id=?", (run["id"],))}
        candidate = {}
        for rec in c.execute("SELECT * FROM raw_records"):
            rid = rec["record_id"]
            if rec["last_event_id"] <= run["high_water"] and rid in replay_rows:
                r = replay_rows[rid]
                candidate[rid] = {
                    "record_id": rid, "package_id": r["package_id"],
                    "record_key": r["record_key"], "status": r["status"],
                    "payload_json": r["payload_json"], "content_hash": r["content_hash"],
                }
            else:  # 追平：重放期间到达或变更的记录，用新规则现算
                row = self._derive_row(spec, rid, {
                    "fields": json.loads(rec["fields_json"]),
                    "status": rec["status"],
                    "origin_package_id": rec["origin_package_id"],
                })
                candidate[rid] = row
        return candidate

    def preview_switch(self, run_id: int) -> dict:
        """切换预检：基于当前最新原始状态（含追平）与当前快照做全量差异。"""
        with self.store.read() as c:
            run = c.execute("SELECT * FROM replay_runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                raise NotFoundError(f"重放不存在: {run_id}")
            if run["status"] != "completed":
                raise ConflictError(f"重放 #{run_id} 状态为 {run['status']}，不能预检")
            snap = self._current_snapshot(c)
            candidate = self._build_candidate(c, run)
            cur_rows = self._row_map(c.execute(
                "SELECT * FROM rows WHERE snapshot_id=?", (snap["id"],)).fetchall())
            cand_map = {rid: {
                "record_id": r["record_id"], "package_id": r["package_id"],
                "record_key": r["record_key"], "status": r["status"],
                "payload": json.loads(r["payload_json"]) if r["payload_json"] else None,
                "content_hash": r["content_hash"],
            } for rid, r in candidate.items()}
            diff = self._diff_maps(cur_rows, cand_map)
            hw_now = self._high_water(c)
            catch_up = sum(1 for rec in c.execute(
                "SELECT last_event_id FROM raw_records")
                if rec["last_event_id"] > run["high_water"])
            diff.update({
                "run_id": run_id, "rule_version": run["rule_version"],
                "current_snapshot_id": snap["id"], "current_rule": snap["rule_version"],
                "high_water_now": hw_now, "catch_up_records": catch_up,
            })
            return diff

    def publish(self, run_id: int, expected_snapshot_id: int,
                expected_high_water: int | None = None,
                actor: str = "operator") -> dict:
        """原子切换：CAS 校验 → 追平 → 封存当前 → 开放新快照 → 移动指针。单事务。"""
        with self.store.write() as c:
            run = c.execute("SELECT * FROM replay_runs WHERE id=?", (run_id,)).fetchone()
            if run is None:
                raise NotFoundError(f"重放不存在: {run_id}")
            if run["status"] != "completed":
                raise ConflictError(
                    f"重放 #{run_id} 状态为 {run['status']}，不能用于发布"
                    + ("（已被另一次发布消费）" if run["status"] == "consumed" else ""))
            snap = self._current_snapshot(c)
            if snap["id"] != expected_snapshot_id:
                self._event(c, "publish_rejected", actor,
                            f"发布被拒绝：期望当前快照 #{expected_snapshot_id}，"
                            f"实际 #{snap['id']}（已有另一次切换）",
                            {"run_id": run_id})
                raise ConflictError("当前快照已变化，请重新预检后再确认发布")
            hw_now = self._high_water(c)
            if expected_high_water is not None and hw_now != expected_high_water:
                self._event(c, "publish_rejected", actor,
                            f"发布被拒绝：高水位 {hw_now} 与预检时 {expected_high_water} 不一致",
                            {"run_id": run_id})
                raise ConflictError("预检后有新数据到达，请重新预检")

            candidate = self._build_candidate(c, run)
            cur_rows = self._row_map(c.execute(
                "SELECT * FROM rows WHERE snapshot_id=?", (snap["id"],)).fetchall())
            cand_map = {rid: {
                "record_id": r["record_id"], "package_id": r["package_id"],
                "record_key": r["record_key"], "status": r["status"],
                "payload": json.loads(r["payload_json"]) if r["payload_json"] else None,
                "content_hash": r["content_hash"],
            } for rid, r in candidate.items()}
            diff = self._diff_maps(cur_rows, cand_map)
            catch_up = sum(1 for rec in c.execute("SELECT last_event_id FROM raw_records")
                           if rec["last_event_id"] > run["high_water"])

            c.execute("UPDATE snapshots SET status='sealed', sealed_at=? WHERE id=?",
                      (_now(), snap["id"]))
            n_pkg = c.execute("SELECT COUNT(*) n FROM packages").fetchone()["n"]
            cur = c.execute(
                "INSERT INTO snapshots(rule_version, status, origin, coverage, high_water,"
                " created_at) VALUES(?,?,?,?,?,?)",
                (run["rule_version"], "open", f"publish:replay:{run_id}", n_pkg, hw_now, _now()))
            new_id = cur.lastrowid
            for rid, row in candidate.items():
                c.execute(
                    "INSERT INTO rows(snapshot_id, record_id, package_id, record_key, status,"
                    " payload_json, content_hash) VALUES(?,?,?,?,?,?,?)",
                    (new_id, rid, row["package_id"], row["record_key"], row["status"],
                     row["payload_json"], row["content_hash"]))
            c.execute("UPDATE pointer SET snapshot_id=? WHERE id=1", (new_id,))
            c.execute("UPDATE replay_runs SET status='consumed' WHERE id=?", (run_id,))
            self._event(c, "snapshot_sealed", actor,
                        f"快照 #{snap['id']}（规则 {snap['rule_version']}）封存",
                        {"snapshot_id": snap["id"]})
            self._event(c, "published", actor,
                        f"发布快照 #{new_id}：规则 {run['rule_version']}，"
                        f"新增 {diff['summary']['added']}，移除 {diff['summary']['removed']}，"
                        f"变更 {diff['summary']['changed']}，追平 {catch_up} 条",
                        {"from_snapshot": snap["id"], "to_snapshot": new_id,
                         "rule_version": run["rule_version"], "run_id": run_id,
                         "diff": diff["summary"], "catch_up": catch_up})
            return {"snapshot_id": new_id, "rule_version": run["rule_version"],
                    "diff": diff["summary"], "catch_up": catch_up}

    def rollback(self, to_snapshot_id: int, expected_snapshot_id: int, reason: str,
                 expected_high_water: int | None = None,
                 actor: str = "operator") -> dict:
        """回滚：用目标封存快照的规则版本对当前全部原始状态重算，生成新开放快照。

        旧快照行保持原样（只读历史），回滚本身也是一次新的、可审计的发布。
        """
        if not reason:
            raise IngestError("回滚必须填写原因")
        with self.store.write() as c:
            target = c.execute("SELECT * FROM snapshots WHERE id=?",
                               (to_snapshot_id,)).fetchone()
            if target is None:
                raise NotFoundError(f"快照不存在: {to_snapshot_id}")
            if target["status"] != "sealed":
                raise ConflictError("只能回滚到已封存的快照")
            snap = self._current_snapshot(c)
            if snap["id"] != expected_snapshot_id:
                raise ConflictError("当前快照已变化，请刷新后重试")
            hw_now = self._high_water(c)
            if expected_high_water is not None and hw_now != expected_high_water:
                raise ConflictError("操作期间有新数据到达，请刷新后重试")

            spec = json.loads(c.execute(
                "SELECT spec_json FROM rule_versions WHERE version=?",
                (target["rule_version"],)).fetchone()["spec_json"])
            candidate = {}
            for rec in c.execute("SELECT * FROM raw_records"):
                row = self._derive_row(spec, rec["record_id"], {
                    "fields": json.loads(rec["fields_json"]),
                    "status": rec["status"],
                    "origin_package_id": rec["origin_package_id"],
                })
                candidate[rec["record_id"]] = row

            c.execute("UPDATE snapshots SET status='sealed', sealed_at=? WHERE id=?",
                      (_now(), snap["id"]))
            n_pkg = c.execute("SELECT COUNT(*) n FROM packages").fetchone()["n"]
            cur = c.execute(
                "INSERT INTO snapshots(rule_version, status, origin, coverage, high_water,"
                " created_at) VALUES(?,?,?,?,?,?)",
                (target["rule_version"], "open", f"rollback:to:{to_snapshot_id}",
                 n_pkg, hw_now, _now()))
            new_id = cur.lastrowid
            for rid, row in candidate.items():
                c.execute(
                    "INSERT INTO rows(snapshot_id, record_id, package_id, record_key, status,"
                    " payload_json, content_hash) VALUES(?,?,?,?,?,?,?)",
                    (new_id, rid, row["package_id"], row["record_key"], row["status"],
                     row["payload_json"], row["content_hash"]))
            c.execute("UPDATE pointer SET snapshot_id=? WHERE id=1", (new_id,))
            self._event(c, "rollback", actor,
                        f"回滚：快照 #{snap['id']}（{snap['rule_version']}）→ 新快照 #{new_id}"
                        f"（{target['rule_version']}，参照封存快照 #{to_snapshot_id}）。原因：{reason}",
                        {"from_snapshot": snap["id"], "to_snapshot": new_id,
                         "reference_snapshot": to_snapshot_id, "reason": reason})
            return {"snapshot_id": new_id, "rule_version": target["rule_version"]}

    def _create_snapshot_from_rule(self, c, rule_version: str, origin: str) -> int:
        spec = json.loads(c.execute("SELECT spec_json FROM rule_versions WHERE version=?",
                                    (rule_version,)).fetchone()["spec_json"])
        hw = self._high_water(c)
        n_pkg = c.execute("SELECT COUNT(*) n FROM packages").fetchone()["n"]
        cur = c.execute(
            "INSERT INTO snapshots(rule_version, status, origin, coverage, high_water,"
            " created_at) VALUES(?,?,?,?,?,?)",
            (rule_version, "open", origin, n_pkg, hw, _now()))
        sid = cur.lastrowid
        for rec in c.execute("SELECT * FROM raw_records"):
            row = self._derive_row(spec, rec["record_id"], {
                "fields": json.loads(rec["fields_json"]),
                "status": rec["status"],
                "origin_package_id": rec["origin_package_id"],
            })
            c.execute(
                "INSERT INTO rows(snapshot_id, record_id, package_id, record_key, status,"
                " payload_json, content_hash) VALUES(?,?,?,?,?,?,?)",
                (sid, rec["record_id"], row["package_id"], row["record_key"], row["status"],
                 row["payload_json"], row["content_hash"]))
        return sid

    # ---------------- 查询 ----------------

    def state(self) -> dict:
        with self.store.read() as c:
            snap = self._current_snapshot(c)
            counts = {}
            for table in ("packages", "raw_records", "raw_pending", "rule_versions",
                          "snapshots", "events"):
                counts[table] = c.execute(f"SELECT COUNT(*) n FROM {table}").fetchone()["n"]
            row_stats = c.execute(
                "SELECT status, COUNT(*) n FROM rows WHERE snapshot_id=? GROUP BY status",
                (snap["id"],)).fetchall()
            replays = [dict(r) for r in c.execute(
                "SELECT id, rule_version, status, high_water, total, done, error,"
                " started_at, finished_at FROM replay_runs ORDER BY id DESC LIMIT 20")]
            events = [dict(r) for r in c.execute(
                "SELECT id, ts, kind, actor, summary FROM events ORDER BY id DESC LIMIT 50")]
            pending = [dict(r) for r in c.execute(
                "SELECT id, event_id, target_record_id, kind, created_at FROM raw_pending"
                " ORDER BY id DESC LIMIT 100")]
            rules = [dict(r) for r in c.execute(
                "SELECT version, note, created_at FROM rule_versions ORDER BY created_at")]
            snapshots = [dict(r) for r in c.execute(
                "SELECT id, rule_version, status, origin, coverage, high_water, created_at,"
                " sealed_at FROM snapshots ORDER BY id DESC LIMIT 20")]
            return {
                "counts": counts,
                "current_snapshot": {
                    "id": snap["id"], "rule_version": snap["rule_version"],
                    "origin": snap["origin"], "coverage": snap["coverage"],
                    "high_water": snap["high_water"], "created_at": snap["created_at"],
                    "rows": {r["status"]: r["n"] for r in row_stats},
                },
                "replays": replays, "events": events, "pending": pending,
                "rules": rules, "snapshots": snapshots,
            }

    def list_packages(self) -> list[dict]:
        return [dict(r) for r in self.store.q(
            "SELECT package_id, sha256, source, seq, received_at, size, n_details,"
            " n_corrections, status FROM packages ORDER BY received_at DESC")]

    def get_package(self, package_id: str) -> dict:
        with self.store.read() as c:
            p = c.execute("SELECT * FROM packages WHERE package_id=?",
                          (package_id,)).fetchone()
            if p is None:
                raise NotFoundError(f"包不存在: {package_id}")
            events = [dict(r) for r in c.execute(
                "SELECT id, kind, record_id, payload_json, created_at FROM raw_events"
                " WHERE package_id=? ORDER BY id", (package_id,))]
            snap = self._current_snapshot(c)
            rows = [dict(r) for r in c.execute(
                "SELECT record_id, record_key, status, payload_json, content_hash FROM rows"
                " WHERE snapshot_id=? AND package_id=?", (snap["id"], package_id))]
            return {"package": {k: p[k] for k in
                                ("package_id", "sha256", "source", "seq", "received_at",
                                 "size", "n_details", "n_corrections", "status",
                                 "manifest_json")},
                    "events": events, "current_rows": rows,
                    "current_snapshot_id": snap["id"]}

    def snapshot_rows(self, snapshot_id: int, limit: int = 200, offset: int = 0,
                      status: str | None = None, q: str | None = None) -> dict:
        with self.store.read() as c:
            snap = c.execute("SELECT * FROM snapshots WHERE id=?", (snapshot_id,)).fetchone()
            if snap is None:
                raise NotFoundError(f"快照不存在: {snapshot_id}")
            where, args = "WHERE snapshot_id=?", [snapshot_id]
            if status:
                where += " AND status=?"
                args.append(status)
            if q:
                where += " AND record_id LIKE ?"
                args.append(f"%{q}%")
            total = c.execute(f"SELECT COUNT(*) n FROM rows {where}", args).fetchone()["n"]
            rows = [dict(r) for r in c.execute(
                f"SELECT record_id, package_id, record_key, status, payload_json, content_hash"
                f" FROM rows {where} ORDER BY record_id LIMIT ? OFFSET ?",
                args + [limit, offset])]
            return {"snapshot": dict(snap), "total": total, "rows": rows}

    def trace_record(self, record_id: str) -> dict:
        with self.store.read() as c:
            rec = c.execute("SELECT * FROM raw_records WHERE record_id=?",
                            (record_id,)).fetchone()
            events = [dict(r) for r in c.execute(
                "SELECT id, package_id, kind, payload_json, created_at FROM raw_events"
                " WHERE record_id=? ORDER BY id", (record_id,))]
            pending = [dict(r) for r in c.execute(
                "SELECT id, event_id, kind, payload_json, created_at FROM raw_pending"
                " WHERE target_record_id=?", (record_id,))]
            rows = [dict(r) for r in c.execute(
                "SELECT r.snapshot_id, s.rule_version, s.status AS snapshot_status,"
                " r.package_id, r.record_key, r.status, r.content_hash, r.payload_json"
                " FROM rows r JOIN snapshots s ON s.id=r.snapshot_id"
                " WHERE r.record_id=? ORDER BY r.snapshot_id", (record_id,))]
            if rec is None and not events:
                raise NotFoundError(f"记录不存在: {record_id}")
            return {
                "record_id": record_id,
                "current": dict(rec) if rec else None,
                "events": events, "pending": pending, "snapshot_rows": rows,
            }

    def list_events(self, limit: int = 200) -> list[dict]:
        return [dict(r) for r in self.store.q(
            "SELECT id, ts, kind, actor, summary, detail_json FROM events"
            " ORDER BY id DESC LIMIT ?", (limit,))]
