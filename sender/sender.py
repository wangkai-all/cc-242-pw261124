#!/usr/bin/env python3
"""本地发送器：构造来源压缩包（manifest + 明细 + 更正）并 POST 到接收工作台。

用法：
  python3 sender.py --url http://127.0.0.1:8000 demo            # 演示剧本（含乱序撤销/重发/冲突）
  python3 sender.py --url http://127.0.0.1:8000 burst --n 5     # 连发 n 个普通包
  python3 sender.py --url http://127.0.0.1:8000 stream --n 8 --interval 2   # 间隔发送（模拟重放期间新包到达）
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import time
import zipfile
from urllib import request, error


def build_package(package_id: str, seq: int, details: list[dict],
                  corrections: list[dict] | None = None,
                  source: str = "local-sender") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps({
            "package_id": package_id, "seq": seq, "source": source,
            "sent_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }, ensure_ascii=False))
        zf.writestr("details.jsonl", "\n".join(
            json.dumps(d, ensure_ascii=False) for d in details))
        if corrections:
            zf.writestr("corrections.jsonl", "\n".join(
                json.dumps(c, ensure_ascii=False) for c in corrections))
    return buf.getvalue()


def send(url: str, blob: bytes, label: str = "") -> dict:
    req = request.Request(url.rstrip("/") + "/api/packages", data=blob, method="POST",
                          headers={"Content-Type": "application/zip",
                                   "X-Source": "local-sender"})
    try:
        with request.urlopen(req, timeout=10) as resp:
            out = json.loads(resp.read().decode("utf-8"))
            print(f"[发送] {label or 'package'} -> {resp.status} {out}")
            return out
    except error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        print(f"[发送] {label or 'package'} -> HTTP {e.code} {body}")
        return {"status": "http_error", "code": e.code, "body": body}


def order(rid: str, date: str, sku: str, qty, price, currency: str,
          channel: str = "web") -> dict:
    return {"record_id": rid, "order_date": date, "sku": sku, "qty": qty,
            "unit_price": price, "currency": currency, "channel": channel}


# ---------------------------------------------------------------- 演示剧本

def demo_packages() -> list[tuple[str, bytes, str]]:
    """返回 [(标签, 字节, 说明)]，覆盖：正常、跨包更正、撤销先于原件、原件后到、重发、冲突。"""
    pkgs = []

    d1 = [
        order("rec-001", "2026-11-01", "SKU-100", 2, 100.0, "CNY"),
        order("rec-002", "02/11/2026", "SKU-101", 1, 50.0, "USD"),   # 混合格式日期
        order("rec-003", "20261103", "SKU-102", 5, 20.0, "EUR"),
        order("rec-004", "2026-11-04", "SKU-100", 1, 999.0, "CNY", channel="test"),  # 测试渠道
        order("rec-005", "2026-11-05", "SKU-103", 3, 33.333, "CNY"),
    ]
    pkgs.append(("PKG-0001", build_package("PKG-0001", 1, d1),
                 "5 条明细（混合日期格式/币种，含 test 渠道）"))

    # 跨包更正 rec-002；撤销 rec-003；撤销 rec-007 —— 但 rec-007 的原件还没到（乱序）
    c2 = [
        {"target": "rec-002", "mode": "merge",
         "fields": {"qty": 4, "unit_price": 45.5}, "reason": "客户改数量"},
        {"target": "rec-003", "mode": "revoke", "reason": "欺诈订单"},
        {"target": "rec-007", "mode": "revoke", "reason": "撤销先于原件到达"},
    ]
    pkgs.append(("PKG-0002", build_package("PKG-0002", 2, [], c2),
                 "跨包更正 rec-002 + 撤销 rec-003 + 撤销未到的 rec-007（应悬挂）"))

    # rec-007 原件晚到：应"出生即撤销"；另带两条新明细
    d3 = [
        order("rec-006", "2026-11-06", "SKU-104", 1, 88.0, "USD"),
        order("rec-007", "2026-11-07", "SKU-105", 2, 10.0, "CNY"),
        order("rec-008", "08/11/2026", "SKU-100", 1, 120.0, "EUR"),
    ]
    pkgs.append(("PKG-0003", build_package("PKG-0003", 3, d3),
                 "rec-007 原件晚到（应直接撤销）+ 两条新明细"))
    return pkgs


def burst_packages(n: int, start_seq: int = 100) -> list[tuple[str, bytes, str]]:
    out = []
    for i in range(n):
        seq = start_seq + i
        rid = f"rec-{seq:04d}"
        details = [
            order(rid, f"2026-11-{10 + i % 18:02d}", f"SKU-{100 + i % 5}",
                  1 + i % 4, round(10.0 + i * 1.7, 2),
                  ["CNY", "USD", "EUR"][i % 3]),
            order(f"{rid}b", f"2026-11-{10 + i % 18:02d}", "SKU-200",
                  1, 5.0 + i, "CNY"),
        ]
        pid = f"PKG-{seq:04d}"
        out.append((pid, build_package(pid, seq, details), f"突发包 {pid}"))
    return out


def main():
    ap = argparse.ArgumentParser(description="本地发送器")
    ap.add_argument("--url", default="http://127.0.0.1:8000")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("demo", help="演示剧本")
    p_burst = sub.add_parser("burst", help="连发普通包")
    p_burst.add_argument("--n", type=int, default=5)
    p_burst.add_argument("--start-seq", type=int, default=100)
    p_stream = sub.add_parser("stream", help="间隔发送（模拟重放期间到达）")
    p_stream.add_argument("--n", type=int, default=8)
    p_stream.add_argument("--interval", type=float, default=2.0)
    p_stream.add_argument("--start-seq", type=int, default=200)
    p_one = sub.add_parser("resend-duplicate", help="同一字节包连发两次（验证只收一次）")
    p_conflict = sub.add_parser("send-conflict", help="同包号不同内容（应被拒绝）")
    args = ap.parse_args()

    if args.cmd == "demo":
        for label, blob, desc in demo_packages():
            print(f"[剧本] {label}: {desc}")
            send(args.url, blob, label)
            time.sleep(0.3)
    elif args.cmd == "burst":
        for label, blob, desc in burst_packages(args.n, args.start_seq):
            send(args.url, blob, label)
    elif args.cmd == "stream":
        for label, blob, desc in burst_packages(args.n, args.start_seq):
            send(args.url, blob, label)
            time.sleep(args.interval)
    elif args.cmd == "resend-duplicate":
        blob = build_package("PKG-DUP-1", 9001,
                             [order("rec-dup-1", "2026-11-09", "SKU-100", 1, 1.0, "CNY")])
        send(args.url, blob, "PKG-DUP-1（第一次）")
        send(args.url, blob, "PKG-DUP-1（字节相同的重发）")
    elif args.cmd == "send-conflict":
        bad = build_package("PKG-0001", 1, [order("rec-999", "2026-11-09",
                                                  "SKU-999", 1, 1.0, "CNY")])
        send(args.url, bad, "PKG-0001（内容不同的冲突包）")


if __name__ == "__main__":
    main()
