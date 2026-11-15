"""SQLite 存储层：单连接 + 写锁，所有写操作在显式事务中完成。

设计要点：
- 单连接（check_same_thread=False）+ RLock，天然串行化写者，避免 SQLite 多连接锁竞争。
- write() 上下文管理器 = BEGIN IMMEDIATE ... COMMIT/ROLLBACK，支持嵌套（仅最外层提交）。
- 所有状态变更（收包、重放落行、发布、回滚）都是单事务，进程在任意点崩溃都不会留下半提交状态。
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS packages (
  package_id     TEXT PRIMARY KEY,
  sha256         TEXT NOT NULL UNIQUE,
  source         TEXT,
  seq            INTEGER,
  received_at    TEXT NOT NULL,
  size           INTEGER NOT NULL,
  n_details      INTEGER NOT NULL DEFAULT 0,
  n_corrections  INTEGER NOT NULL DEFAULT 0,
  status         TEXT NOT NULL DEFAULT 'received',
  raw            BLOB NOT NULL,
  manifest_json  TEXT NOT NULL
);

-- 原始层事件日志（append-only，规则无关）。重放的"截至某时点状态"由它折叠重建。
CREATE TABLE IF NOT EXISTS raw_events (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  package_id   TEXT NOT NULL,
  kind         TEXT NOT NULL,              -- detail | correction | revocation
  record_id    TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at   TEXT NOT NULL
);

-- 原始层当前状态（= fold(raw_events) 的物化缓存）。
CREATE TABLE IF NOT EXISTS raw_records (
  record_id         TEXT PRIMARY KEY,
  fields_json       TEXT NOT NULL,
  status            TEXT NOT NULL,         -- active | revoked
  origin_package_id TEXT NOT NULL,
  last_event_id     INTEGER NOT NULL,
  updated_at        TEXT NOT NULL
);

-- 目标尚未到达的更正/撤销（跨包乱序）。原件到达时在同事务内解决。
CREATE TABLE IF NOT EXISTS raw_pending (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id         INTEGER NOT NULL UNIQUE,
  target_record_id TEXT NOT NULL,
  kind             TEXT NOT NULL,          -- correction | revocation
  payload_json     TEXT NOT NULL,
  created_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rule_versions (
  version    TEXT PRIMARY KEY,
  spec_json  TEXT NOT NULL,
  note       TEXT,
  created_at TEXT NOT NULL
);

-- 快照：任一时刻恰好一个 open；sealed 快照及其行永不被修改（只读历史）。
CREATE TABLE IF NOT EXISTS snapshots (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_version TEXT NOT NULL,
  status       TEXT NOT NULL,              -- open | sealed
  origin       TEXT NOT NULL,              -- init | publish:replay:N | rollback:to:N
  coverage     INTEGER NOT NULL,           -- 覆盖的包数（信息展示）
  high_water   INTEGER NOT NULL,           -- 覆盖到的 raw_event 高水位
  created_at   TEXT NOT NULL,
  sealed_at    TEXT
);

-- 派生结果行：每条结果可追溯到 (快照, 规则版本, 原包, 记录)。
CREATE TABLE IF NOT EXISTS rows (
  snapshot_id  INTEGER NOT NULL,
  record_id    TEXT NOT NULL,
  package_id   TEXT NOT NULL,              -- 记录的原包（溯源）
  record_key   TEXT,
  status       TEXT NOT NULL,              -- active | revoked | filtered
  payload_json TEXT,
  content_hash TEXT NOT NULL,
  PRIMARY KEY (snapshot_id, record_id)
);

-- 当前开放快照指针（单行）。
CREATE TABLE IF NOT EXISTS pointer (
  id          INTEGER PRIMARY KEY CHECK (id = 1),
  snapshot_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS replay_runs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_version  TEXT NOT NULL,
  status        TEXT NOT NULL,             -- running | completed | interrupted | failed | consumed
  high_water    INTEGER NOT NULL,          -- 启动时冻结的 raw_event 高水位
  coverage_json TEXT NOT NULL,             -- 启动时冻结的包清单
  total         INTEGER NOT NULL DEFAULT 0,
  done          INTEGER NOT NULL DEFAULT 0,
  error         TEXT,
  started_at    TEXT NOT NULL,
  finished_at   TEXT
);

CREATE TABLE IF NOT EXISTS replay_rows (
  run_id       INTEGER NOT NULL,
  record_id    TEXT NOT NULL,
  package_id   TEXT NOT NULL,
  record_key   TEXT,
  status       TEXT NOT NULL,
  payload_json TEXT,
  content_hash TEXT NOT NULL,
  PRIMARY KEY (run_id, record_id)
);

-- 审计事件（append-only）：任何状态变化都留痕，恢复流程也写事件。
CREATE TABLE IF NOT EXISTS events (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  ts          TEXT NOT NULL,
  kind        TEXT NOT NULL,
  actor       TEXT NOT NULL,
  summary     TEXT NOT NULL,
  detail_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_rows_snapshot ON rows(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_record ON raw_events(record_id);
CREATE INDEX IF NOT EXISTS idx_events_id ON events(id);
"""


class Store:
    """单连接 + 写锁的 SQLite 封装。"""

    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.isolation_level = None  # 手动控制事务
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._lock = threading.RLock()
        self._tx_depth = 0
        self.conn.executescript(SCHEMA)

    @contextmanager
    def write(self):
        """写事务：BEGIN IMMEDIATE，嵌套时仅最外层真正提交。"""
        with self._lock:
            outermost = self._tx_depth == 0
            if outermost:
                self.conn.execute("BEGIN IMMEDIATE")
            self._tx_depth += 1
            try:
                yield self.conn
                self._tx_depth -= 1
                if outermost:
                    self.conn.execute("COMMIT")
            except BaseException:
                self._tx_depth -= 1
                if outermost:
                    self.conn.execute("ROLLBACK")
                raise

    @contextmanager
    def read(self):
        """读一致性：若在写事务内则复用同一事务（读到自己的未提交写）。"""
        with self._lock:
            yield self.conn

    def q(self, sql: str, args=()) -> list[sqlite3.Row]:
        with self.read() as c:
            return c.execute(sql, args).fetchall()

    def q1(self, sql: str, args=()) -> sqlite3.Row | None:
        with self.read() as c:
            return c.execute(sql, args).fetchone()

    def close(self):
        with self._lock:
            self.conn.close()
