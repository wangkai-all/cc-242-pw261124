"use strict";

const $ = (sel) => document.querySelector(sel);
let STATE = null;
let CURRENT_TAB = "overview";

async function api(path, opts = {}) {
  const resp = await fetch(path, opts);
  const text = await resp.text();
  let body;
  try { body = JSON.parse(text); } catch { body = { error: text }; }
  if (!resp.ok) {
    const err = new Error(body.error || `HTTP ${resp.status}`);
    err.status = resp.status;
    throw err;
  }
  return body;
}

function toast(msg, isErr = false) {
  let el = document.querySelector(".toast");
  if (!el) {
    el = document.createElement("div");
    el.className = "toast";
    document.body.appendChild(el);
  }
  el.textContent = msg;
  el.className = "toast show" + (isErr ? " err" : "");
  setTimeout(() => el.classList.remove("show"), 3200);
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function chip(text, cls = "") { return `<span class="chip ${cls}">${esc(text)}</span>`; }

function statusChip(s) {
  const map = {
    ok: "ok", parsed: "ok", completed: "ok", active: "ok", open: "ok",
    running: "warn", received: "warn", revoked: "bad", failed: "bad",
    interrupted: "bad", duplicate: "grey", consumed: "grey", sealed: "grey",
    filtered: "grey",
  };
  return chip(s, map[s] || "");
}

function modal(title, html) {
  $("#modal-title").textContent = title;
  $("#modal-body").innerHTML = html;
  $("#modal-mask").classList.remove("hidden");
}
function closeModal() { $("#modal-mask").classList.add("hidden"); }

// ---------------------------------------------------------------- 总览

function renderOverview() {
  const c = STATE.counts, cs = STATE.current_snapshot;
  $("#overview-cards").innerHTML = [
    [c.packages, "已收数据包"], [c.raw_records, "原始记录"],
    [c.raw_pending, "悬挂更正/撤销"], [c.rule_versions, "规则版本"],
    [c.snapshots, "快照数"], [c.events, "审计事件"],
  ].map(([n, l]) => `<div class="card"><div class="num">${n}</div><div class="label">${l}</div></div>`).join("");

  const rows = Object.entries(cs.rows).map(([k, v]) => `${statusChip(k)} ${v}`).join("　");
  $("#current-snapshot").innerHTML = `<dl class="kv">
    <dt>快照</dt><dd>#${cs.id}（规则 <b>${esc(cs.rule_version)}</b>，来源 ${esc(cs.origin)}）</dd>
    <dt>行数</dt><dd>${rows || "（空）"}</dd>
    <dt>覆盖</dt><dd>${cs.coverage} 个包，高水位 ${cs.high_water}</dd>
    <dt>开放时间</dt><dd class="mono">${esc(cs.created_at)}</dd>
  </dl>`;

  $("#pending-list").innerHTML = STATE.pending.length === 0
    ? '<p class="muted">无悬挂：所有更正/撤销的目标记录均已到达。</p>'
    : `<table><tr><th>目标记录</th><th>类型</th><th>事件</th><th>时间</th></tr>` +
      STATE.pending.map(p => `<tr><td class="mono">${esc(p.target_record_id)}</td>
        <td>${statusChip(p.kind)}</td><td class="mono">#${p.event_id}</td>
        <td class="mono">${esc(p.created_at)}</td></tr>`).join("") + "</table>";

  $("#recent-events").innerHTML = "<table><tr><th>#</th><th>时间</th><th>类型</th><th>操作者</th><th>摘要</th></tr>" +
    STATE.events.slice(0, 15).map(e => `<tr><td class="mono">${e.id}</td>
      <td class="mono">${esc(e.ts)}</td><td>${esc(e.kind)}</td>
      <td>${esc(e.actor)}</td><td>${esc(e.summary)}</td></tr>`).join("") + "</table>";
}

// ---------------------------------------------------------------- 数据包

async function renderPackages() {
  const { packages } = await api("/api/packages");
  $("#packages-table").innerHTML = packages.length === 0
    ? '<p class="muted">尚未收到数据包。用 sender/sender.py 发送。</p>'
    : `<table><tr><th>包号</th><th>序号</th><th>接收时间</th><th>明细</th><th>更正</th>
       <th>大小</th><th>状态</th><th>sha256</th></tr>` +
      packages.map(p => `<tr class="clickable" data-pkg="${esc(p.package_id)}">
        <td class="mono">${esc(p.package_id)}</td><td>${p.seq ?? ""}</td>
        <td class="mono">${esc(p.received_at)}</td><td>${p.n_details}</td><td>${p.n_corrections}</td>
        <td>${p.size}</td><td>${statusChip(p.status)}</td>
        <td class="mono">${esc(p.sha256.slice(0, 12))}…</td></tr>`).join("") + "</table>";
  document.querySelectorAll("[data-pkg]").forEach(tr =>
    tr.addEventListener("click", () => showPackage(tr.dataset.pkg)));
}

async function showPackage(pid) {
  try {
    const d = await api(`/api/packages/${encodeURIComponent(pid)}`);
    const m = JSON.parse(d.package.manifest_json);
    const evRows = d.events.map(e => `<tr><td class="mono">#${e.id}</td><td>${statusChip(e.kind)}</td>
      <td class="mono">${esc(e.record_id)}</td><td class="mono">${esc(e.payload_json)}</td></tr>`).join("");
    const rowRows = d.current_rows.map(r => `<tr><td class="mono">${esc(r.record_id)}</td>
      <td>${statusChip(r.status)}</td><td class="mono">${esc(r.record_key)}</td>
      <td class="mono">${esc((r.payload_json || "").slice(0, 120))}</td></tr>`).join("");
    modal(`数据包 ${pid}`, `
      <dl class="kv">
        <dt>manifest</dt><dd class="mono">${esc(JSON.stringify(m))}</dd>
        <dt>sha256</dt><dd class="mono">${esc(d.package.sha256)}</dd>
        <dt>接收时间</dt><dd class="mono">${esc(d.package.received_at)}</dd>
      </dl>
      <h3>原始事件（${d.events.length}）</h3>
      <table><tr><th>#</th><th>类型</th><th>记录</th><th>负载</th></tr>${evRows || "<tr><td colspan=4 class=muted>无</td></tr>"}</table>
      <h3>当前快照 #${d.current_snapshot_id} 中来自本包的行（${d.current_rows.length}）</h3>
      <table><tr><th>记录</th><th>状态</th><th>业务键</th><th>内容（截断）</th></tr>${rowRows || "<tr><td colspan=4 class=muted>无</td></tr>"}</table>`);
  } catch (e) { toast(e.message, true); }
}

// ---------------------------------------------------------------- 规则

const V2_EXAMPLE = {
  version: "v2",
  key_field: "record_id",
  transforms: [
    { op: "rename", from: "order_date", to: "date" },
    { op: "date_norm", field: "date", formats: ["%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"] },
    { op: "round", field: "unit_price", digits: 2 },
    { op: "upper", field: "currency" },
    { op: "convert_currency", field: "unit_price", currency_field: "currency",
      to: "CNY", rates: { CNY: 1.0, USD: 7.2, EUR: 7.8 }, out_field: "unit_price_cny" },
    { op: "compute", out_field: "amount_cny", expr: "qty*unit_price_cny", round: 2 },
  ],
  filters: [{ field: "channel", op: "eq", value: "test" }],
};

function renderRules() {
  $("#rules-list").innerHTML = "<table><tr><th>版本</th><th>备注</th><th>注册时间</th></tr>" +
    STATE.rules.map(r => `<tr><td class="mono"><b>${esc(r.version)}</b></td>
      <td>${esc(r.note || "")}</td><td class="mono">${esc(r.created_at)}</td></tr>`).join("") + "</table>";
}

// ---------------------------------------------------------------- 重放与发布

function renderReplay() {
  const sel = $("#replay-rule");
  const cur = STATE.current_snapshot.rule_version;
  sel.innerHTML = STATE.rules.filter(r => r.version !== cur)
    .map(r => `<option value="${esc(r.version)}">${esc(r.version)}</option>`).join("")
    || `<option value="">（无其他规则可重放）</option>`;

  $("#replays-table").innerHTML = STATE.replays.length === 0
    ? '<p class="muted">还没有重放任务。</p>'
    : `<table><tr><th>#</th><th>规则</th><th>状态</th><th>进度</th><th>高水位</th>
       <th>开始</th><th>结束</th><th>操作</th></tr>` +
      STATE.replays.map(r => {
        const pct = r.total ? Math.round(100 * r.done / r.total) : 0;
        const btns = [];
        if (r.status === "completed") {
          btns.push(`<button class="small" data-diff="${r.id}">差异</button>`);
          btns.push(`<button class="small" data-preview="${r.id}">预检并发布</button>`);
        }
        if (r.status === "running")
          btns.push(`<button class="small ghost" data-interrupt="${r.id}">中断</button>`);
        if (r.status === "interrupted")
          btns.push(`<button class="small" data-resume="${r.id}">续跑</button>`);
        return `<tr><td class="mono">#${r.id}</td><td class="mono">${esc(r.rule_version)}</td>
          <td>${statusChip(r.status)}</td>
          <td><div class="progress"><div style="width:${pct}%"></div></div>
              <span class="muted">${r.done}/${r.total}</span></td>
          <td class="mono">${r.high_water}</td>
          <td class="mono">${esc(r.started_at)}</td><td class="mono">${esc(r.finished_at || "")}</td>
          <td>${btns.join(" ")}</td></tr>`;
      }).join("") + "</table>";

  $("#snapshots-table").innerHTML =
    `<table><tr><th>#</th><th>规则</th><th>状态</th><th>来源</th><th>覆盖包数</th>
     <th>高水位</th><th>创建</th><th>封存</th><th>操作</th></tr>` +
    STATE.snapshots.map(s => `<tr><td class="mono">#${s.id}</td>
      <td class="mono">${esc(s.rule_version)}</td><td>${statusChip(s.status)}</td>
      <td class="mono">${esc(s.origin)}</td><td>${s.coverage}</td>
      <td class="mono">${s.high_water}</td>
      <td class="mono">${esc(s.created_at)}</td><td class="mono">${esc(s.sealed_at || "")}</td>
      <td>${s.status === "sealed"
        ? `<button class="small ghost" data-rows="${s.id}">查看行</button>
           <button class="small danger" data-rollback="${s.id}" data-rule="${esc(s.rule_version)}">回滚到此</button>`
        : `<button class="small ghost" data-rows="${s.id}">查看行</button>`}</td></tr>`).join("")
    + "</table>";

  document.querySelectorAll("[data-diff]").forEach(b =>
    b.addEventListener("click", () => showDiff(+b.dataset.diff)));
  document.querySelectorAll("[data-preview]").forEach(b =>
    b.addEventListener("click", () => showPreview(+b.dataset.preview)));
  document.querySelectorAll("[data-interrupt]").forEach(b =>
    b.addEventListener("click", () => action(`/api/replays/${b.dataset.interrupt}/interrupt`, "已中断")));
  document.querySelectorAll("[data-resume]").forEach(b =>
    b.addEventListener("click", () => action(`/api/replays/${b.dataset.resume}/resume`, "已续跑")));
  document.querySelectorAll("[data-rows]").forEach(b =>
    b.addEventListener("click", () => showSnapshotRows(+b.dataset.rows)));
  document.querySelectorAll("[data-rollback]").forEach(b =>
    b.addEventListener("click", () => confirmRollback(+b.dataset.rollback, b.dataset.rule)));
}

function diffTable(diff) {
  const sec = (title, rows, cls, render) =>
    `<h3 class="${cls}">${title}（${rows.length}）</h3>` +
    (rows.length ? `<table>${rows.map(render).join("")}</table>` : '<p class="muted">无</p>');
  return `
    <p>重放 #${diff.run_id}（规则 <b>${esc(diff.rule_version)}</b>） vs
       当前快照 #${diff.current_snapshot_id}（规则 <b>${esc(diff.current_rule)}</b>）。
       高水位 ${diff.high_water}；重放期间新到/变更 <b>${diff.uncovered_records ?? diff.catch_up_records ?? 0}</b> 条（切换时自动用新规则追平）。</p>
    ${sec("新增", diff.added, "diff-add", r => `<tr><td class="mono">${esc(r.record_id)}</td>
      <td>${statusChip(r.status)}</td><td class="mono">${esc(JSON.stringify(r.payload)?.slice(0, 140))}</td></tr>`)}
    ${sec("移除", diff.removed, "diff-del", r => `<tr><td class="mono">${esc(r.record_id)}</td>
      <td>${statusChip(r.status)}</td><td class="mono">${esc(JSON.stringify(r.payload)?.slice(0, 140))}</td></tr>`)}
    ${sec("变更", diff.changed, "diff-chg", r => `<tr><td class="mono">${esc(r.record_id)}</td>
      <td>${esc(r.old_status)} → ${esc(r.new_status)}</td>
      <td class="mono">${esc(JSON.stringify(r.fields).slice(0, 200))}</td></tr>`)}`;
}

async function showDiff(runId) {
  try {
    const diff = await api(`/api/replays/${runId}/diff`);
    modal(`重放 #${runId} 与当前快照的差异`, diffTable(diff));
  } catch (e) { toast(e.message, true); }
}

async function showPreview(runId) {
  try {
    const diff = await api(`/api/replays/${runId}/preview`);
    modal(`切换预检（重放 #${runId} → 规则 ${diff.rule_version}）`, `
      <p>这是<strong>此刻</strong>切换的全量效果（含对重放期间到达数据的追平）。
      确认后将原子切换：封存当前快照 #${diff.current_snapshot_id}，开放新快照。</p>
      <p>追平记录：<b>${diff.catch_up_records}</b> 条；当前高水位：
      <span class="mono">${diff.high_water_now}</span></p>
      ${diffTable(diff)}
      <div class="row">
        <button id="btn-confirm-publish">确认发布（原子切换）</button>
        <button class="ghost" id="btn-cancel-publish">再想想</button>
      </div>`);
    $("#btn-cancel-publish").addEventListener("click", closeModal);
    $("#btn-confirm-publish").addEventListener("click", async () => {
      try {
        const r = await api(`/api/replays/${runId}/publish`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            expected_snapshot_id: diff.current_snapshot_id,
            expected_high_water: diff.high_water_now,
          }),
        });
        closeModal();
        toast(`已发布快照 #${r.snapshot_id}（规则 ${r.rule_version}），追平 ${r.catch_up} 条`);
        refresh();
      } catch (e) {
        closeModal();
        toast(`发布被拒绝：${e.message}`, true);
        refresh();
      }
    });
  } catch (e) { toast(e.message, true); }
}

async function showSnapshotRows(sid) {
  try {
    const d = await api(`/api/snapshots/${sid}/rows?limit=300`);
    modal(`快照 #${sid} 的行（规则 ${d.snapshot.rule_version}，共 ${d.total} 条）`,
      `<table><tr><th>记录</th><th>原包</th><th>业务键</th><th>状态</th><th>内容哈希</th><th>内容</th></tr>` +
      d.rows.map(r => `<tr><td class="mono">${esc(r.record_id)}</td>
        <td class="mono">${esc(r.package_id)}</td><td class="mono">${esc(r.record_key)}</td>
        <td>${statusChip(r.status)}</td><td class="mono">${esc(r.content_hash.slice(0, 10))}…</td>
        <td class="mono">${esc((r.payload_json || "").slice(0, 120))}</td></tr>`).join("") + "</table>");
  } catch (e) { toast(e.message, true); }
}

function confirmRollback(sid, rule) {
  modal("确认回滚", `
    <p>将以封存快照 #${sid} 的规则版本 <b>${esc(rule)}</b> 对当前全部原始数据重算，
    生成新的开放快照。当前快照会被封存保留，不会丢失。</p>
    <div class="row"><input id="rollback-reason" placeholder="回滚原因（必填）" size="50"></div>
    <div class="row">
      <button class="danger" id="btn-do-rollback">确认回滚</button>
      <button class="ghost" id="btn-cancel-rollback">取消</button>
    </div>`);
  $("#btn-cancel-rollback").addEventListener("click", closeModal);
  $("#btn-do-rollback").addEventListener("click", async () => {
    const reason = $("#rollback-reason").value.trim();
    try {
      const r = await api("/api/rollback", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          to_snapshot_id: sid,
          expected_snapshot_id: STATE.current_snapshot.id,
          reason,
        }),
      });
      closeModal();
      toast(`已回滚：新快照 #${r.snapshot_id}（规则 ${r.rule_version}）`);
      refresh();
    } catch (e) { toast(`回滚失败：${e.message}`, true); }
  });
}

async function action(path, okMsg) {
  try {
    await api(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
    toast(okMsg);
  } catch (e) { toast(e.message, true); }
  refresh();
}

// ---------------------------------------------------------------- 追溯

async function doTrace() {
  const rid = $("#trace-record-id").value.trim();
  if (!rid) return;
  try {
    const d = await api(`/api/records/${encodeURIComponent(rid)}/trace`);
    const evRows = d.events.map(e => `<tr><td class="mono">#${e.id}</td>
      <td class="mono">${esc(e.package_id)}</td><td>${statusChip(e.kind)}</td>
      <td class="mono">${esc(e.payload_json)}</td><td class="mono">${esc(e.created_at)}</td></tr>`).join("");
    const snapRows = d.snapshot_rows.map(r => `<tr><td class="mono">#${r.snapshot_id}</td>
      <td class="mono">${esc(r.rule_version)}</td><td>${statusChip(r.snapshot_status)}</td>
      <td class="mono">${esc(r.package_id)}</td><td>${statusChip(r.status)}</td>
      <td class="mono">${esc(r.content_hash.slice(0, 12))}…</td></tr>`).join("");
    const cur = d.current
      ? `<dl class="kv"><dt>当前状态</dt><dd>${statusChip(d.current.status)}</dd>
         <dt>原包</dt><dd class="mono">${esc(d.current.origin_package_id)}</dd>
         <dt>当前字段</dt><dd class="mono">${esc(d.current.fields_json)}</dd></dl>`
      : '<p class="muted">记录尚未在原始层物化（可能只有悬挂事件）。</p>';
    const pending = d.pending.length
      ? `<p class="diff-chg">有 ${d.pending.length} 条悬挂更正/撤销等待原件。</p>` : "";
    $("#trace-result").innerHTML = `
      <h3>记录 ${esc(rid)}</h3>${cur}${pending}
      <h3>原始事件时间线（${d.events.length}）</h3>
      <table><tr><th>#</th><th>来自包</th><th>类型</th><th>负载</th><th>时间</th></tr>
        ${evRows || "<tr><td colspan=5 class=muted>无</td></tr>"}</table>
      <h3>各快照中的结果行（${d.snapshot_rows.length}）</h3>
      <table><tr><th>快照</th><th>规则</th><th>快照状态</th><th>原包</th><th>行状态</th><th>内容哈希</th></tr>
        ${snapRows || "<tr><td colspan=6 class=muted>无</td></tr>"}</table>`;
  } catch (e) {
    $("#trace-result").innerHTML = `<p class="diff-del">${esc(e.message)}</p>`;
  }
}

// ---------------------------------------------------------------- 事件

async function renderEvents() {
  const { events } = await api("/api/events");
  $("#events-table").innerHTML =
    "<table><tr><th>#</th><th>时间</th><th>类型</th><th>操作者</th><th>摘要</th></tr>" +
    events.map(e => `<tr><td class="mono">${e.id}</td><td class="mono">${esc(e.ts)}</td>
      <td>${esc(e.kind)}</td><td>${esc(e.actor)}</td><td>${esc(e.summary)}</td></tr>`).join("")
    + "</table>";
}

// ---------------------------------------------------------------- 框架

async function refresh() {
  try {
    STATE = await api("/api/state");
    $("#head-status").textContent =
      `当前快照 #${STATE.current_snapshot.id} · 规则 ${STATE.current_snapshot.rule_version}`;
    if (CURRENT_TAB === "overview") renderOverview();
    else if (CURRENT_TAB === "packages") await renderPackages();
    else if (CURRENT_TAB === "rules") renderRules();
    else if (CURRENT_TAB === "replay") renderReplay();
    else if (CURRENT_TAB === "events") await renderEvents();
  } catch (e) {
    $("#head-status").textContent = `连接失败：${e.message}`;
  }
}

function main() {
  document.querySelectorAll("#tabs button").forEach(b =>
    b.addEventListener("click", () => {
      document.querySelectorAll("#tabs button").forEach(x => x.classList.remove("active"));
      document.querySelectorAll(".tab").forEach(x => x.classList.remove("active"));
      b.classList.add("active");
      CURRENT_TAB = b.dataset.tab;
      $(`#tab-${CURRENT_TAB}`).classList.add("active");
      refresh();
    }));
  $("#modal-close").addEventListener("click", closeModal);
  $("#modal-mask").addEventListener("click", (e) => {
    if (e.target.id === "modal-mask") closeModal();
  });
  $("#btn-register-rule").addEventListener("click", async () => {
    try {
      const r = await api("/api/rules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ spec: $("#rule-spec").value, note: $("#rule-note").value }),
      });
      $("#rule-register-result").innerHTML = `<p class="diff-add">已注册规则 ${esc(r.version)}</p>`;
      refresh();
    } catch (e) {
      $("#rule-register-result").innerHTML = `<p class="diff-del">${esc(e.message)}</p>`;
    }
  });
  $("#btn-fill-v2").addEventListener("click", () => {
    $("#rule-spec").value = JSON.stringify(V2_EXAMPLE, null, 2);
  });
  $("#btn-start-replay").addEventListener("click", async () => {
    const v = $("#replay-rule").value;
    if (!v) return toast("没有可重放的规则版本", true);
    try {
      const r = await api("/api/replays", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rule_version: v }),
      });
      toast(`重放 #${r.run_id} 已启动`);
    } catch (e) { toast(e.message, true); }
    refresh();
  });
  $("#btn-trace").addEventListener("click", doTrace);
  $("#trace-record-id").addEventListener("keydown", (e) => {
    if (e.key === "Enter") doTrace();
  });
  refresh();
  setInterval(refresh, 2000);
}

main();
