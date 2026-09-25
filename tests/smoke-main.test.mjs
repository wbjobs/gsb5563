// 冒烟：用桩环境加载 main.js，验证模块接线、降级路径与一次完整模拟不抛错。
import { SSEParser } from '../lib/parser.js';

const handlers = {};
function makeEl() {
  return {
    hidden: false, textContent: '', value: '', checked: false, disabled: false,
    className: '', innerHTML: '', files: [], style: {},
    addEventListener(ev, fn) { handlers[this._id + ':' + ev] = fn; },
    appendChild() {}, replaceChildren() {},
    getContext: () => makeCtx(), getBoundingClientRect: () => ({ width: 800, height: 220, left: 0, top: 0 }),
  };
}
function makeCtx() {
  return new Proxy({}, { get: (t, k) => (k === 'measureText' ? () => ({ width: 0 }) : () => {}), set: () => true });
}

const ids = {};
function el(id) { const e = ids[id] || (ids[id] = makeEl()); e._id = id; return e; }

// 假 Worker：主线程真实解析
class StubWorker {
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
        reply({ type: 'ready', stats: this.parser.stats });
      } else if (msg.type === 'feed') {
        this.parser.feed(new Uint8Array(msg.bytes));
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
        reply({ type: 'ended', seq: msg.seq, ...b, lastEventId: this.parser.lastEventId, stats: this.parser.stats });
      }
    });
  }
}

globalThis.window = { addEventListener() {}, devicePixelRatio: 1 };
globalThis.document = {
  getElementById: el,
  createElement: () => makeEl(),
  createDocumentFragment: () => ({ appendChild() {} }),
};
globalThis.performance = { now: () => Number(process.hrtime.bigint() / 1000n) / 1000 };
globalThis.requestAnimationFrame = (fn) => { try { fn(16); } catch (_) {} return 0; };
globalThis.cancelAnimationFrame = () => {};
globalThis.setTimeout = setTimeout; globalThis.clearTimeout = clearTimeout;
globalThis.Worker = StubWorker;
globalThis.EventSource = function () {};
globalThis.ResizeObserver = class { observe() {} unobserve() {} };
globalThis.indexedDB = undefined; // 强制走“IDB 不可用”降级分支

const mod = await import('../js/main.js');
await new Promise((r) => setTimeout(r, 50));

// 触发一次完整模拟（开始按钮处理器）
el('rawInput').value = [
  'retry: 30', 'id: m1', 'data: hello', '',
  ': hb', 'id: m2', 'data: a', 'data: b', '',
  'badfield: x', 'retry: no',
  'id: m3', 'data: c', '',
  ':!disconnect',
  'id: m4', 'data: lost',
  ':!expect-id m4',
  'id: m5', 'data: resumed', '',
  'id: m5', 'data: dup', '',
].join('\n');
el('optSpeed').value = '100000';
el('optChunk').value = '8';
el('optChaos').checked = true;
el('optMaxRetry').value = '5';
await handlers['btnStart:click']();
await new Promise((r) => setTimeout(r, 50));
console.log('start: dispatched =', el('stDispatched').textContent, 'dup =', el('stDup').textContent,
            'hb =', el('stHb').textContent, 'badRetry =', el('stBadRetry').textContent,
            'state =', el('statePill').textContent, 'lastId =', el('lastEventId').textContent);
if (Number(el('stDispatched').textContent) !== 4) throw new Error('expected 4 dispatched, got ' + el('stDispatched').textContent);
if (Number(el('stDup').textContent) !== 1) throw new Error('expected 1 duplicate, got ' + el('stDup').textContent);
if (Number(el('stBadRetry').textContent) !== 1) throw new Error('expected 1 invalid retry, got ' + el('stBadRetry').textContent);
if (el('statePill').textContent !== 'DONE') throw new Error('expected DONE');
console.log('full simulation smoke passed');
