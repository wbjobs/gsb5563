// 端到端：compileScript + 真 SSEParser + Simulator（假 Worker 承载解析器）+ 状态机
import { SSEParser } from '../lib/parser.js';
import { Simulator } from '../lib/simulator.js';
import { SSEStateMachine, State } from '../lib/fsm.js';
import { compileScript } from '../lib/compile.js';

class FakeWorker {
  constructor() { this.onmessage = null; }
  postMessage(msg) {
    queueMicrotask(() => {
      const reply = (m) => this.onmessage({ data: m });
      if (msg.type === 'init') {
        this.parser = new SSEParser({
          onEvent: (ev) => this.pending.events.push(ev),
          onComment: (text, line) => this.pending.comments.push({ text, line }),
          onRetry: (ms, line) => this.pending.retries.push({ ms, line }),
        });
        this.parser.lastEventId = String(msg.lastEventId || '');
        this.pending = { events: [], comments: [], retries: [] };
        reply({ type: 'ready', lastEventId: this.parser.lastEventId });
      } else if (msg.type === 'feed') {
        let bytes = msg.bytes;
        if (bytes instanceof ArrayBuffer) bytes = new Uint8Array(bytes);
        this.parser.feed(bytes);
        const b = this.pending;
        this.pending = { events: [], comments: [], retries: [] };
        reply({ type: 'batch', seq: msg.seq, ...b, lastEventId: this.parser.lastEventId, stats: this.parser.stats, parseMs: 0 });
      } else if (msg.type === 'reset') {
        this.parser.resetStream();
        reply({ type: 'reset-done', seq: msg.seq, lastEventId: this.parser.lastEventId });
      } else if (msg.type === 'end') {
        this.parser.endStream();
        const b = this.pending;
        this.pending = { events: [], comments: [], retries: [] };
        reply({ type: 'ended', seq: msg.seq, ...b, lastEventId: this.parser.lastEventId, stats: this.parser.stats, parseMs: 0 });
      }
    });
  }
}

let passed = 0;
let failed = 0;
function assert(cond, name, extra) {
  if (cond) passed++;
  else { failed++; console.error('FAIL:', name, extra ?? ''); }
}

const script = [
  'retry: 50',
  'id: a1',
  'data: first',
  '',
  ': heartbeat',
  'id: a2',
  'data: multi1',
  'data: multi2',
  '',
  'retry: nope',
  'bogus: x',
  'id: a3',
  'data: before-disconnect',
  '',
  ':!disconnect',
  'id: a4-never-dispatched',
  'data: lost',
  ':!expect-id a3',
  'id: a5',
  'data: resumed',
  '',
  'id: a5',
  'data: dup',
  '',
  'id: a6',
  'data: final',
  '',
].join('\n');

const { actions, sessions } = compileScript(script);
assert(sessions === 2, 'compiler splits 2 sessions');
assert(actions.some((a) => a.type === 'disconnect'), 'compiler emits disconnect');
assert(actions.some((a) => a.type === 'expect-id' && a.value === 'a3'), 'compiler emits expect-id');

const fsm = new SSEStateMachine();
const received = [];
const seen = new Set();
let dups = 0;
let expectFails = 0;
let reconnectCarried = [];
const sim = new Simulator(new FakeWorker(), fsm, {
  onBatch: (msg) => {
    for (const ev of msg.events) {
      if (ev.id && seen.has(ev.id)) { dups++; continue; }
      if (ev.id) seen.add(ev.id);
      received.push(ev);
    }
  },
  onEntry: (it) => {
    if (it.kind === 'reconnect-attempt' || it.kind === 'connecting') reconnectCarried.push(it.carriedId);
  },
  onSessionEnd: () => {},
  onExpectFail: () => expectFails++,
});

await sim.run(script, { speed: 100000, chunkSize: 16, chaos: true, maxRetries: 8 });

assert(fsm.state === State.DONE, 'ends in DONE, got ' + fsm.state);
const ids = received.map((e) => e.id);
assert(JSON.stringify(ids) === JSON.stringify(['a1', 'a2', 'a3', 'a5', 'a6']), 'event ids after dedup/discard', JSON.stringify(ids));
assert(received[1].data === 'multi1\nmulti2', 'multiline data');
assert(!ids.includes('a4-never-dispatched'), 'undispatched pre-disconnect event lost');
assert(dups === 1, 'duplicate a5 removed once, got ' + dups);
assert(expectFails === 0, 'Last-Event-ID carried correctly on reconnect');
assert(reconnectCarried.length === 2, 'initial + reconnect attempts');
assert(reconnectCarried[1] === 'a3', 'reconnect carries a3 (last valid id), got ' + reconnectCarried[1]);
assert(fsm.attempt === 0, 'attempt counter reset after successful open');

// 非法 retry 后仍能达到最大重试上限
{
  const fsm2 = new SSEStateMachine();
  let closes = 0;
  const sim2 = new Simulator(new FakeWorker(), fsm2, {
    onBatch: () => {}, onEntry: () => {}, onSessionEnd: () => {}, onExpectFail: () => {},
    onFatal: () => { closes++; },
  });
  const s2 = [
    'retry: abc', 'data: x', '',
    ':!disconnect',
    ':!expect-id ', 'data: y', '',
    ':!disconnect',
    'data: z', '',
  ].join('\n');
  await sim2.run(s2, { speed: 1000000, chunkSize: 32, maxRetries: 1 });
  assert(closes === 1 && fsm2.state === State.CLOSED, 'reconnect limit -> CLOSED, got ' + fsm2.state + ' closes=' + closes);
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
