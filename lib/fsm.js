// SSE 客户端生命周期状态机
// CONNECTING → OPEN →（断线）→ RETRY_WAIT →（重连）→ CONNECTING → OPEN …
// OPEN 期间的心跳不改变状态，只在时序上打点。
export const State = Object.freeze({
  CONNECTING: 'CONNECTING',
  OPEN: 'OPEN',
  RETRY_WAIT: 'RETRY_WAIT',
  CLOSED: 'CLOSED',
  DONE: 'DONE',
});

const MAX_BACKOFF_MS = 30000;

export class SSEStateMachine {
  constructor(onChange) {
    this.state = State.CONNECTING;
    this.attempt = 0;
    this.retryMs = 3000;
    this.lastEventId = '';
    this.onChange = onChange || null;
  }

  _set(state, extra = {}) {
    const prev = this.state;
    this.state = state;
    if (this.onChange) this.onChange(state, prev, extra);
  }

  connecting(carriedId) {
    if (typeof carriedId === 'string') this.lastEventId = carriedId;
    this._set(State.CONNECTING, { attempt: this.attempt, carriedId: this.lastEventId });
  }

  open() {
    this._set(State.OPEN);
    this.attempt = 0;
  }

  // 返回退避后的下一次重连等待毫秒（指数退避 + ±15% 抖动，上限 30s）
  disconnectAndSchedule() {
    this.attempt += 1;
    const attempt = this.attempt;
    const raw = this.retryMs * 2 ** (attempt - 1);
    const base = Math.min(raw, MAX_BACKOFF_MS);
    const clamped = raw > MAX_BACKOFF_MS;
    const jitter = base * 0.15 * (Math.random() * 2 - 1);
    const delay = Math.max(0, Math.round(base + jitter));
    this._set(State.RETRY_WAIT, { attempt, delay, clamped });
    return { attempt, delay, base, clamped };
  }

  close(reason) {
    this._set(State.CLOSED, { reason });
  }

  done() {
    this._set(State.DONE);
  }
}
