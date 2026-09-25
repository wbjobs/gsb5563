// 模拟服务端：把编译后的动作按时序喂给 Worker 中的解析器，
// 并驱动 SSE 状态机完成 连接→接收→心跳→断线→retry退避重连(携带 Last-Event-ID) 的生命周期。
import { compileScript } from './compile.js';

export class Simulator {
  constructor(worker, fsm, hooks = {}) {
    this.worker = worker;
    this.fsm = fsm;
    this.hooks = hooks; // { onBatch, onEntry, onState, onExpectFail, onFatal }
    this.running = false;
    this.paused = false;
    this.stopRequested = false;
    this._seq = 0;
    this._waiters = null;
    this._resetWaiters = null;
    this._bindWorker();
  }

  _bindWorker() {
    this.worker.onmessage = (e) => {
      const msg = e.data;
      if (msg.type === 'batch' || msg.type === 'ended') {
        if (this.hooks.onBatch) this.hooks.onBatch(msg);
      } else if (msg.type === 'ready') {
        if (this.hooks.onReady) this.hooks.onReady(msg);
        if (this._waiters) { const w = this._waiters; this._waiters = null; w(); }
      } else if (msg.type === 'reset-done') {
        // 断线会话的解析器保留了断线前最后一个有效 id，主线程以此续传
        if (typeof msg.lastEventId === 'string') this.fsm.lastEventId = msg.lastEventId;
        if (this._resetWaiters) { const w = this._resetWaiters; this._resetWaiters = null; w(); }
      }
    };
  }

  async run(text, opts) {
    const { actions, sessions } = compileScript(text);
    this.running = true;
    this.paused = false;
    this.stopRequested = false;

    const bySession = new Map();
    for (const a of actions) {
      if (!bySession.has(a.session)) bySession.set(a.session, []);
      bySession.get(a.session).push(a);
    }

    for (let session = 0; session < sessions; session++) {
      if (this.stopRequested) break;
      const acts = bySession.get(session) || [];

      // 达到最大重连次数则不再发起新会话
      if (session > 0 && opts.maxRetries && this.fsm.attempt >= opts.maxRetries) {
        this.fsm.close('超过最大重连次数');
        if (this.hooks.onFatal) this.hooks.onFatal('reconnect-limit');
        this.running = false;
        return;
      }
      // 发起连接（重连时携带 Last-Event-ID 请求头）
      this.fsm.connecting(this.fsm.lastEventId);
      if (this.hooks.onEntry) {
        this.hooks.onEntry({
          kind: session === 0 ? 'connecting' : 'reconnect-attempt',
          session,
          carriedId: this.fsm.lastEventId,
        });
      }
      await this._ready();
      if (this.stopRequested) break;
      this.fsm.open();
      if (this.hooks.onEntry) this.hooks.onEntry({ kind: 'connected', session });

      let disconnected = false;
      for (const act of acts) {
        await this._gate();
        if (this.stopRequested) break;
        if (act.type === 'feed') {
          await this._feed(act.text, opts);
        } else if (act.type === 'delay') {
          if (this.hooks.onEntry) this.hooks.onEntry({ kind: 'server-silent', session, ms: act.ms });
          await this._sleep(act.ms / opts.speed);
        } else if (act.type === 'expect-id') {
          const actual = this.fsm.lastEventId;
          if (actual !== act.value) {
            if (this.hooks.onExpectFail) this.hooks.onExpectFail(act, actual);
          } else if (this.hooks.onEntry) {
            this.hooks.onEntry({ kind: 'expect-id-ok', session, value: actual });
          }
        } else if (act.type === 'disconnect') {
          disconnected = true;
          if (this.hooks.onEntry) {
            this.hooks.onEntry({ kind: 'dropped-note', session, note: '连接被服务端中断：未以空行结束的事件按规范丢弃，lastEventId 保留' });
          }
          if (this.hooks.onSessionEnd) this.hooks.onSessionEnd(session, 'disconnected');
          // 通知 Worker 断流并取回它保留的 lastEventId（断线前 id 行已生效）
          await new Promise((resolve) => {
            this._resetWaiters = resolve;
            this.worker.postMessage({ type: 'reset', seq: this._seq++ });
          });
          const sch = this.fsm.disconnectAndSchedule();
          if (this.hooks.onEntry) {
            this.hooks.onEntry({
              kind: 'reconnect-wait',
              session: session + 1,
              attempt: sch.attempt,
              delayMs: sch.delay,
              baseMs: sch.base,
              clamped: sch.clamped,
              carriedId: this.fsm.lastEventId,
            });
          }
          await this._sleep(sch.delay / opts.speed);
          break;
        }
      }
      if (this.stopRequested) break;

      if (!disconnected) {
        // 会话自然结束：通知 Worker 收尾派发
        this.worker.postMessage({ type: 'end', seq: this._seq++ });
        await this._sleep(0);
        if (this.hooks.onSessionEnd) this.hooks.onSessionEnd(session, 'complete');
      }
    }

    this.running = false;
    if (!this.stopRequested) {
      this.fsm.done();
      if (this.hooks.onEntry) this.hooks.onEntry({ kind: 'done' });
    }
  }

  async _ready() {
    await new Promise((resolve) => {
      this._waiters = resolve;
      this.worker.postMessage({ type: 'init', lastEventId: this.fsm.lastEventId });
    });
  }

  async _feed(text, opts) {
    const bytes = new TextEncoder().encode(text);
    let pos = 0;
    let posted = 0;
    while (pos < bytes.length) {
      let n = opts.chunkSize || 256;
      if (opts.chaos) n = Math.max(1, Math.min(n, 1 + Math.floor(Math.random() * n)));
      const chunk = bytes.slice(pos, pos + n);
      pos += n;
      // 复制后 transfer，避免把更大的底层 buffer 所有权转走
      const buf = chunk.buffer.slice(chunk.byteOffset, chunk.byteOffset + chunk.byteLength);
      this.worker.postMessage({ type: 'feed', bytes: buf, seq: this._seq++ }, [buf]);
      posted++;
      if (posted % 256 === 0) await this._tick();
    }
    await this._tick();
  }

  _tick() {
    return new Promise((r) => setTimeout(r, 0));
  }

  async _sleep(ms) {
    const deadline = performance.now() + Math.max(0, ms);
    while (true) {
      if (this.stopRequested) return;
      const remaining = deadline - performance.now();
      if (remaining <= 0) return;
      if (this.paused) {
        await new Promise((r) => { this._resumeWait = r; });
        continue;
      }
      await new Promise((r) => {
        this._sleepHandle = setTimeout(r, Math.min(100, remaining));
      });
    }
  }

  async _gate() {
    while (this.paused && !this.stopRequested) {
      await new Promise((r) => { this._resumeWait = r; });
    }
  }

  pause() { this.paused = true; }

  resume() {
    this.paused = false;
    if (this._resumeWait) { const w = this._resumeWait; this._resumeWait = null; w(); }
  }

  stop() {
    this.stopRequested = true;
    this.resume();
    if (this._sleepHandle) clearTimeout(this._sleepHandle);
    this.fsm.close('用户手动停止');
    this.running = false;
  }
}
