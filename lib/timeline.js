// 时序数据模型：内存中保存全部时序条目（供 Canvas 绘制），
// 事件日志额外保留最近 5000 条供列表查看；完整日志由 IndexedDB 持久化。
const RECENT_EVENT_LIMIT = 5000;

export class TimelineStore {
  constructor() {
    this.entries = [];
    this.recentEvents = [];
    this.sessions = [];
    this.seq = 0;
    this.counters = {
      received: 0,
      dispatched: 0,
      duplicates: 0,
      heartbeats: 0,
      reconnects: 0,
      invalidRetries: 0,
      invalidFields: 0,
    };
    this.listeners = new Set();
  }

  subscribe(fn) {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  _emit() {
    for (const fn of this.listeners) fn(this);
  }

  add(item) {
    item.ts = item.ts || performance.now();
    item.seq = this.seq++;
    this.entries.push(item);
    if (item.kind === 'event') {
      this.recentEvents.push(item);
      if (this.recentEvents.length > RECENT_EVENT_LIMIT) this.recentEvents.shift();
    }
    this._emit();
  }

  bulkAdd(items) {
    for (const item of items) {
      item.ts = item.ts || performance.now();
      item.seq = this.seq++;
      this.entries.push(item);
      if (item.kind === 'event') {
        this.recentEvents.push(item);
      }
    }
    while (this.recentEvents.length > RECENT_EVENT_LIMIT) this.recentEvents.shift();
    this._emit();
  }

  startSession(session) {
    const span = { session, openTs: null, closeTs: null, outcome: null };
    this.sessions.push(span);
    return span;
  }

  endSession(session, outcome, ts) {
    for (let i = this.sessions.length - 1; i >= 0; i--) {
      const sp = this.sessions[i];
      if (sp.session === session) {
        sp.outcome = outcome;
        sp.closeTs = ts || performance.now();
        return;
      }
    }
  }

  clear() {
    this.entries = [];
    this.recentEvents = [];
    this.sessions = [];
    this.seq = 0;
    this.counters = {
      received: 0,
      dispatched: 0,
      duplicates: 0,
      heartbeats: 0,
      reconnects: 0,
      invalidRetries: 0,
      invalidFields: 0,
    };
    this._emit();
  }
}
