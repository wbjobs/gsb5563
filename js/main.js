// 主控：装配 Worker 解析、状态机、模拟器、IndexedDB 日志与 Canvas 时序。
import { SSEStateMachine, State } from '../lib/fsm.js';
import { Simulator } from '../lib/simulator.js';
import { EventLogDB } from '../lib/db.js';
import { TimelineStore } from '../lib/timeline.js';
import { TimelineView } from './timeline-view.js';
import { demoScript, stressScript } from './samples.js';
import { SSEParser, DEFAULT_RETRY_MS } from '../lib/parser.js';

const $ = (id) => document.getElementById(id);

const els = {
  banner: $('fallbackBanner'),
  raw: $('rawInput'),
  start: $('btnStart'),
  pause: $('btnPause'),
  stop: $('btnStop'),
  demo: $('btnLoadDemo'),
  stress: $('btnLoadStress'),
  file: $('fileInput'),
  speed: $('optSpeed'),
  chunk: $('optChunk'),
  chaos: $('optChaos'),
  maxRetry: $('optMaxRetry'),
  pill: $('statePill'),
  lastEventId: $('lastEventId'),
  retryMs: $('retryMs'),
  attempt: $('attempt'),
  sessions: $('sessions'),
  stReceived: $('stReceived'),
  stDispatched: $('stDispatched'),
  stDup: $('stDup'),
  stHb: $('stHb'),
  stInvalid: $('stInvalid'),
  stBadRetry: $('stBadRetry'),
  stParseMs: $('stParseMs'),
  notice: $('noticeBar'),
  follow: $('chkFollow'),
  tooltip: $('canvasTooltip'),
  filter: $('filterInput'),
  tbody: $('logTbody'),
  detail: $('detailPane'),
  exportBtn: $('btnExport'),
  clearDb: $('btnClearDb'),
  dbCount: $('dbCount'),
};

// ---------- 能力检测与降级 ----------
let worker = null;
let workerDegraded = false;
if (typeof EventSource === 'undefined') {
  els.banner.hidden = false;
  els.banner.textContent = '⚠ 当前浏览器不支持 EventSource：真实页面应降级为 fetch + ReadableStream 或 XHR 轮询；本工具不联网，仍可在下方离线模拟完整 SSE 行为。';
}
try {
  if (typeof Worker === 'undefined') throw new Error('no Worker');
  worker = new Worker(new URL('../worker/sse-worker.js', import.meta.url), { type: 'module' });
} catch (err) {
  workerDegraded = true;
  worker = createMainThreadParserShim();
  const extra = typeof EventSource === 'undefined'
    ? ' 同时检测到 Worker 不可用，解析已降级到主线程（万条数据可能出现短暂卡顿）。'
    : ' 解析已降级到主线程执行（Web Worker 不可用），小数据量功能不受影响。';
  els.banner.hidden = false;
  els.banner.textContent = '⚠ 浏览器不支持（或被禁用）Web Worker。' + extra;
}

// 主线程降级解析器：与 Worker 保持同一消息协议，保证上层状态机逻辑不变。
function createMainThreadParserShim() {
  let parser = null;
  let pending = { events: [], comments: [], retries: [] };
  const target = {
    postMessage(msg) {
      queueMicrotask(() => {
        const reply = (m) => target.onmessage && target.onmessage({ data: m });
        if (msg.type === 'init') {
          parser = new SSEParser({
            onEvent: (ev) => pending.events.push(ev),
            onComment: (text, line) => pending.comments.push({ text, line }),
            onRetry: (ms, line) => pending.retries.push({ ms, line }),
          });
          parser.lastEventId = String(msg.lastEventId || '');
          reply({ type: 'ready', lastEventId: parser.lastEventId, stats: parser.stats });
        } else if (msg.type === 'feed') {
          let bytes = msg.bytes;
          if (bytes instanceof ArrayBuffer) bytes = new Uint8Array(bytes);
          parser.feed(bytes);
          const batch = pending;
          pending = { events: [], comments: [], retries: [] };
          reply({ type: 'batch', seq: msg.seq, ...batch, lastEventId: parser.lastEventId, stats: parser.stats, parseMs: 0 });
        } else if (msg.type === 'reset') {
          parser.resetStream();
          reply({ type: 'reset-done', seq: msg.seq, lastEventId: parser.lastEventId });
        } else if (msg.type === 'end') {
          parser.endStream();
          const batch = pending;
          pending = { events: [], comments: [], retries: [] };
          reply({ type: 'ended', seq: msg.seq, ...batch, lastEventId: parser.lastEventId, stats: parser.stats, parseMs: 0 });
        }
      });
    },
  };
  return target;
}

// ---------- 核心装配 ----------
const db = new EventLogDB();
const timeline = new TimelineStore();
const fsm = new SSEStateMachine(renderState);
let view = null;
let simulator = null;
const seenIds = new Set();
let totalParseMs = 0;
let dbReady = false;

const persistQueue = [];
let persistTimer = 0;
function queuePersist(items) {
  if (!dbReady) return;
  persistQueue.push(...items);
  if (persistTimer) return;
  persistTimer = setTimeout(flushPersist, 250);
}
async function flushPersist() {
  persistTimer = 0;
  if (!persistQueue.length) return;
  const items = persistQueue.splice(0, persistQueue.length);
  try {
    await db.bulkPutEvents(items);
    await db.putSeenIds(seenIds);
    await db.putMeta('lastEventId', fsm.lastEventId);
    updateDbCount();
  } catch (e) {
    console.warn('IDB persist failed', e);
  }
}
async function updateDbCount() {
  if (!dbReady) return;
  try { els.dbCount.textContent = `IDB 已存 ${await db.countEvents()} 条事件`; } catch (_) {}
}

function entry(kind, extra = {}) {
  timeline.add({ kind, label: extra.label || '', detail: extra.detail || '', ...extra });
}

// 处理 Worker 回传的一批解析结果：去重、心跳识别、retry 更新、非法报文统计。
function handleBatch(msg) {
  const stats = msg.stats;
  if (stats) {
    fsm.lastEventId = msg.lastEventId || '';
  }
  totalParseMs += msg.parseMs || 0;
  els.stParseMs.textContent = totalParseMs.toFixed(1) + 'ms';

  const c = timeline.counters;
  const newEntries = [];
  const persist = [];

  // 按起始行合并展示顺序：注释（心跳）与事件交错处理
  const stream = [
    ...msg.comments.map((cm) => ({ t: 'c', line: cm.line, item: cm })),
    ...msg.events.map((ev) => ({ t: 'e', line: ev.startLine, item: ev })),
    ...msg.retries.map((rt) => ({ t: 'r', line: rt.line, item: rt })),
  ].sort((a, b) => a.line - b.line);

  for (const node of stream) {
    if (node.t === 'c') {
      c.heartbeats++;
      newEntries.push({ kind: 'heartbeat', detail: '注释行：' + node.item.text });
    } else if (node.t === 'r') {
      fsm.retryMs = node.item.ms;
      els.retryMs.textContent = node.item.ms + 'ms（服务端指定）';
      newEntries.push({ kind: 'retry-wait', label: 'retry=' + node.item.ms + 'ms', detail: `第 ${node.item.line} 行：重连间隔更新为 ${node.item.ms}ms` });
    } else {
      const ev = node.item;
      c.received++;
      if (ev.id !== '' && seenIds.has(ev.id)) {
        c.duplicates++;
        newEntries.push({ kind: 'duplicate', detail: `重复事件 ID 已丢弃：${ev.id}（data: ${preview(ev.data)}）`, session: currentSession });
        continue;
      }
      if (ev.id !== '') seenIds.add(ev.id);
      c.dispatched++;
      const row = {
        kind: 'event',
        id: ev.id,
        eventType: ev.event,
        data: ev.data,
        startLine: ev.startLine,
        endLine: ev.endLine,
        session: currentSession,
      };
      newEntries.push(row);
      persist.push({ seq: 0, ts: performance.now(), ...row });
    }
  }

  // 非法字段 / 非法 retry 的统计差量（解析器累计值）
  if (stats) {
    const dBadRetry = stats.invalidRetries - prevStats.invalidRetries;
    for (let i = 0; i < dBadRetry; i++) newEntries.push({ kind: 'invalid-retry', detail: 'retry 字段非纯数字：忽略，保留现有重连间隔（首次为默认 3000ms）' });
    c.invalidFields = statsBase.invalidFields + stats.invalidFields;
    c.invalidRetries = statsBase.invalidRetries + stats.invalidRetries;
    prevStats = { ...stats };
  }

  timeline.bulkAdd(newEntries);
  // 持久化项与 newEntries 中的 event 一一对应，补齐 bulkAdd 分配的 seq/ts
  let pi = 0;
  for (const ne of newEntries) {
    if (ne.kind === 'event') {
      if (persist[pi]) { persist[pi].seq = ne.seq; persist[pi].ts = ne.ts; pi++; }
    }
  }
  queuePersist(persist);
  renderStats();
  renderLastEventId();
}

function preview(s) { return s.length > 60 ? s.slice(0, 60) + '…' : s; }

let prevStats = { invalidFields: 0, invalidRetries: 0 };
const statsBase = { invalidFields: 0, invalidRetries: 0 };
let currentSession = 0;

// ---------- 模拟器回调 ----------
function buildSimulator() {
  return new Simulator(worker, fsm, {
    onBatch: (msg) => handleBatch(msg),
    onReady: () => {
      // 每个会话都是全新解析器：把此前累计作为基线，保证统计与差量跨会话连续
      statsBase.invalidFields = timeline.counters.invalidFields;
      statsBase.invalidRetries = timeline.counters.invalidRetries;
      prevStats.invalidFields = 0;
      prevStats.invalidRetries = 0;
    },
    onEntry: (it) => {
      if (it.kind === 'connecting' || it.kind === 'reconnect-attempt') {
        currentSession = it.session;
        timeline.startSession(it.session);
        const span = timeline.sessions[timeline.sessions.length - 1];
        if (span) span.openTs = performance.now();
        const carried = it.carriedId || '';
        notice(it.kind === 'connecting'
          ? `会话 ${it.session}：发起连接请求${carried ? `，请求头 Last-Event-ID: ${carried}` : '（无 Last-Event-ID）'}`
          : `会话 ${it.session}：发起重连，请求头 Last-Event-ID: ${carried || '（空）'}`);
        entry(it.kind, { session: it.session, detail: `Last-Event-ID=${carried || '(空)'}` });
      } else if (it.kind === 'connected') {
        notice(`会话 ${it.session}：连接已建立（OPEN）`);
        entry('connected', { session: it.session });
      } else if (it.kind === 'reconnect-wait') {
        timeline.counters.reconnects++;
        notice(`将在 ${it.delayMs}ms 后进行第 ${it.attempt} 次重连（指数退避${it.clamped ? '，已触顶 30s' : ''}），携带 Last-Event-ID: ${it.carriedId || '（空）'}`);
        entry('reconnect-wait', {
          session: it.session,
          label: `第${it.attempt}次 ${it.delayMs}ms`,
          detail: `退避基准 ${it.baseMs}ms（±15% 抖动），重连请求携带 Last-Event-ID: ${it.carriedId || '(空)'}`,
        });
      } else {
        entry(it.kind, it);
      }
      renderState();
    },
    onSessionEnd: (session, outcome) => {
      timeline.endSession(session, outcome);
      renderState();
    },
    onExpectFail: (act, actual) => {
      entry('expect-id-fail', {
        session: act.session,
        detail: `第 ${act.line} 行期望 Last-Event-ID="${act.value}"，实际="${actual}"`,
      });
      notice(`⚠ 会话 ${act.session} 续传校验失败：期望 "${act.value}"，实际 "${actual}"`);
    },
    onFatal: (reason) => notice('⛔ ' + (reason === 'reconnect-limit' ? '超过最大重连次数，连接进入 CLOSED' : reason)),
  });
}

// ---------- 渲染 ----------
function renderState() {
  const state = fsm.state;
  if (state === State.CONNECTING && timeline.sessions.length === 0) {
    els.pill.textContent = 'IDLE';
    els.pill.className = 'pill';
  } else {
    els.pill.textContent = state;
    els.pill.className = 'pill ' + state;
  }
  els.attempt.textContent = fsm.attempt;
  els.sessions.textContent = timeline.sessions.length;
}
function renderLastEventId() {
  els.lastEventId.textContent = fsm.lastEventId === '' ? '（空）' : fsm.lastEventId;
}
function renderStats() {
  const c = timeline.counters;
  els.stReceived.textContent = c.received;
  els.stDispatched.textContent = c.dispatched;
  els.stDup.textContent = c.duplicates;
  els.stHb.textContent = c.heartbeats;
  els.stInvalid.textContent = c.invalidFields;
  els.stBadRetry.textContent = c.invalidRetries;
}
let noticeTimer = 0;
function notice(msg) {
  els.notice.textContent = msg;
  clearTimeout(noticeTimer);
  noticeTimer = setTimeout(() => { els.notice.textContent = ''; }, 6000);
}

// 事件列表：仅渲染最近 5000 条 + 过滤，避免主线程被 DOM 拖慢。
let listRaf = 0;
function scheduleListRender() {
  if (listRaf) return;
  listRaf = requestAnimationFrame(() => { listRaf = 0; renderList(); });
}
function renderList() {
  const q = els.filter.value.trim().toLowerCase();
  const rows = timeline.recentEvents;
  const frag = document.createDocumentFragment();
  let shown = 0;
  for (let i = rows.length - 1; i >= 0 && shown < 400; i--) {
    const r = rows[i];
    if (q && !(`${r.id} ${r.eventType} ${r.data}`.toLowerCase().includes(q))) continue;
    shown++;
    const tr = document.createElement('tr');
    if (r.seq === selectedSeq) tr.className = 'selected';
    const td = (txt) => { const d = document.createElement('td'); d.textContent = txt; return d; };
    tr.appendChild(td(r.seq));
    tr.appendChild(td(new Date(r.ts).toLocaleTimeString() + '.' + String(Math.round(r.ts) % 1000).padStart(3, '0')));
    tr.appendChild(td(r.session));
    tr.appendChild(td(r.eventType));
    tr.appendChild(td(r.id));
    const dc = document.createElement('td');
    dc.className = 'data-cell';
    dc.textContent = r.data;
    tr.appendChild(dc);
    tr.addEventListener('click', () => selectEvent(r.seq));
    frag.appendChild(tr);
  }
  els.tbody.replaceChildren(frag);
}

let selectedSeq = null;
function selectEvent(seq) {
  selectedSeq = seq;
  if (view) view.select(seq);
  const r = timeline.entries.find((e) => e.seq === seq && e.kind === 'event');
  if (r) {
    els.detail.hidden = false;
    let prettyData;
    try { prettyData = JSON.stringify(JSON.parse(r.data), null, 2); } catch (_) { prettyData = r.data; }
    els.detail.textContent = [
      `seq      : ${r.seq}`,
      `session  : ${r.session}`,
      `event    : ${r.eventType}`,
      `id       : ${r.id === '' ? '(空)' : r.id}`,
      `行号     : ${r.startLine}-${r.endLine}`,
      '',
      'data (多行按 SSE 规范以 \\n 拼接):',
      prettyData,
    ].join('\n');
  }
  renderList();
}

// ---------- 控件 ----------
els.demo.addEventListener('click', () => { els.raw.value = demoScript(); els.speed.value = '16'; notice('已载入演示脚本，点击“开始模拟”'); });
els.stress.addEventListener('click', () => {
  els.raw.value = stressScript(10000);
  els.speed.value = '1000';
  els.chunk.value = 4096;
  els.chaos.checked = false;
  notice('已生成 1 万条事件脚本（含中途断流、非法 retry、注释、未知字段、重复 ID）');
});
els.file.addEventListener('change', async () => {
  const f = els.file.files[0];
  if (!f) return;
  els.raw.value = await f.text();
  notice(`已加载本地文件 ${f.name}（${f.size} 字节）`);
});
els.filter.addEventListener('input', renderList);
els.follow.addEventListener('change', () => view && view.setFollow(els.follow.checked));

els.start.addEventListener('click', startSim);
els.pause.addEventListener('click', () => {
  if (!simulator) return;
  if (simulator.paused) { simulator.resume(); els.pause.textContent = '暂停'; }
  else { simulator.pause(); els.pause.textContent = '继续'; }
});
els.stop.addEventListener('click', () => {
  if (simulator) simulator.stop();
  afterStop();
  notice('已手动停止');
});

function options() {
  return {
    speed: Number(els.speed.value) || 1,
    chunkSize: Math.max(1, Number(els.chunk.value) || 64),
    chaos: els.chaos.checked,
    maxRetries: Number(els.maxRetry.value) || 0,
  };
}

async function startSim() {
  const text = els.raw.value;
  if (!text.trim()) { notice('请先输入或加载 SSE 报文'); return; }
  timeline.clear();
  seenIds.clear();
  totalParseMs = 0;
  currentSession = 0;
  prevStats.invalidFields = 0;
  prevStats.invalidRetries = 0;
  statsBase.invalidFields = 0;
  statsBase.invalidRetries = 0;
  fsm.attempt = 0;
  fsm.retryMs = DEFAULT_RETRY_MS;
  fsm.lastEventId = '';
  renderLastEventId();
  els.retryMs.textContent = DEFAULT_RETRY_MS + 'ms（默认）';
  renderStats();
  renderState();
  simulator = buildSimulator();
  els.start.disabled = true;
  els.pause.disabled = false;
  els.pause.textContent = '暂停';
  els.stop.disabled = false;
  const t0 = performance.now();
  await simulator.run(text, options());
  afterStop();
  await flushPersist();
  notice(`模拟结束，共处理 ${timeline.counters.dispatched} 条事件，Worker 后台解析累计 ${totalParseMs.toFixed(1)}ms，墙钟 ${(performance.now() - t0).toFixed(0)}ms`);
}

function afterStop() {
  els.start.disabled = false;
  els.pause.disabled = true;
  els.pause.textContent = '暂停';
  els.stop.disabled = true;
}

els.exportBtn.addEventListener('click', () => {
  const data = timeline.recentEvents.map(({ kind, label, detail, ...rest }) => rest);
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'sse-events.json';
  a.click();
  URL.revokeObjectURL(a.href);
});
els.clearDb.addEventListener('click', async () => {
  if (!dbReady) return;
  await db.clearEvents();
  timeline.clear();
  seenIds.clear();
  fsm.lastEventId = '';
  renderLastEventId();
  renderStats();
  renderList();
  updateDbCount();
  notice('IndexedDB 事件日志已清空');
});

// Canvas 按需重绘：条目变化时节流刷新；无变化也用 rAF 驱动（会话结束时间/跟随）。
let viewDirty = true;
let lastViewDraw = 0;
timeline.subscribe(() => { viewDirty = true; scheduleListRender(); });
function viewLoop(now) {
  if (view) {
    if (viewDirty) { view.draw(); viewDirty = false; lastViewDraw = now; }
    else if (view.follow && now - lastViewDraw > 80) { view.draw(); lastViewDraw = now; }
  }
  requestAnimationFrame(viewLoop);
}

// ---------- 启动 ----------
(async function init() {
  view = new TimelineView($('timelineCanvas'), els.tooltip, timeline);
  view.onSelect((seq) => selectEvent(seq));
  requestAnimationFrame(viewLoop);
  try {
    await db.open();
    dbReady = true;
    const [saved, savedId] = await Promise.all([db.getAllEvents(), db.getMeta('lastEventId')]);
    if (savedId != null) fsm.lastEventId = String(savedId);
    if (saved.length) {
      for (const it of saved) {
        if (it.id) seenIds.add(it.id);
      }
      timeline.bulkAdd(saved);
      timeline.counters.dispatched = saved.length;
      timeline.counters.received = saved.length;
    }
    renderLastEventId();
    renderStats();
    updateDbCount();
    if (saved.length) notice(`已从 IndexedDB 恢复 ${saved.length} 条历史事件日志（Last-Event-ID 用于下次续传演示）`);
  } catch (e) {
    console.warn('IndexedDB 不可用，日志不持久化', e);
  }
  renderState();
  renderList();
})();
