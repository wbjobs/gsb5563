import { SSEParser } from '../lib/parser.js';
import { compileScript } from '../lib/compile.js';
import { demoScript, stressScript } from '../js/samples.js';

let passed = 0;
let failed = 0;
function assert(cond, name, extra) {
  if (cond) passed++;
  else { failed++; console.error('FAIL:', name, extra ?? ''); }
}

// 用编译器把 demo 拆成会话，再逐会话用新的 SSEParser（携带上一会话的 id）跑一遍
const raw = demoScript();
const { actions, sessions } = compileScript(raw);
assert(sessions === 3, `demo has 3 sessions, got ${sessions}`);

const allEvents = [];
let carriedId = '';
let invalidRetryTotal = 0;
let unknownFields = 0;
let comments = 0;
const expectByIdSession = new Map();
for (const a of actions) if (a.type === 'expect-id') expectByIdSession.set(a.session, a.value);

for (let session = 0; session < sessions; session++) {
  const parser = new SSEParser({
    onEvent: (ev) => allEvents.push({ ...ev, session }),
    onComment: () => comments++,
  });
  parser.lastEventId = carriedId;
  for (const a of actions) {
    if (a.session !== session) continue;
    if (a.type === 'feed') parser.feed(new TextEncoder().encode(a.text));
    if (a.type === 'expect-id') {
      assert(parser.lastEventId === a.value, `session ${session} carries Last-Event-ID ${a.value}, got "${parser.lastEventId}"`);
    }
  }
  parser.endStream();
  invalidRetryTotal += parser.stats.invalidRetries;
  carriedId = parser.lastEventId;
}

const ids = allEvents.map((e) => e.id);
assert(ids[0] === 'evt-1', 'first event id');
assert(allEvents.some((e) => e.data === '这是第一行\n这是第二行(冒号后无空格)\n 第三行保留一个前导空格'), 'multi-line join in demo');
assert(!ids.some((id) => id === ''), 'no empty-id events dispatched');
assert(invalidRetryTotal >= 2, `invalid retries counted (${invalidRetryTotal})`);
assert(comments >= 2, 'comment/heartbeat lines seen');
// 断线前未以空行结束的事件不应出现
assert(!allEvents.some((e) => e.id === 'evt-6'), 'evt-6 (no blank line before disconnect) dropped');
assert(!allEvents.some((e) => e.data.startsWith('断线前未以空行结束 —— 本事件应被丢弃$')), 'pre-disconnect unfinished events dropped');
assert(allEvents.filter((e) => e.session > 0 && e.id === 'evt-7').length === 1, 'evt-7 exactly once (no data leakage across disconnect)');
assert(allEvents.filter((e) => e.session > 1 && e.id === 'evt-11').length === 1, 'evt-11 exactly once');
// 重连后事件正常
assert(ids.includes('evt-7') && ids.includes('evt-11') && ids.includes('evt-12'), 'post-reconnect events present');

// 压测脚本：10000 条、含 2 个会话
const stress = stressScript(10000);
const compiled = compileScript(stress);
assert(compiled.sessions === 2, 'stress splits at embedded disconnect');
const p = new SSEParser({ onEvent: () => {} });
const t0 = Date.now();
for (const a of compiled.actions) if (a.type === 'feed') p.feed(new TextEncoder().encode(a.text));
p.endStream();
assert(p.stats.events >= 10000, `stress events >= 10000, got ${p.stats.events}`);
assert(p.stats.invalidRetries >= 1, 'stress contains invalid retry');
assert(Date.now() - t0 < 2000, `stress parse under 2s (${Date.now() - t0}ms)`);

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
