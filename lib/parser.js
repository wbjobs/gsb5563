// 严格按 HTML SSE 规范实现的流解析器（WHATWG HTML §9.2.6 Interpreting an event stream）
// 可增量喂入 Uint8Array（TypedArray），内部维护跨 chunk 的行缓冲。
// 同一份代码同时被 Web Worker 与 Node 测试脚本复用。

const DEFAULT_RETRY_MS = 3000;

export class SSEParser {
  constructor({ onEvent, onComment, onRetry, onStats } = {}) {
    this._decoder = new TextDecoder('utf-8', { ignoreBOM: true });
    this._lineBuffer = '';
    this._atStart = true;
    this._bomChecked = false;
    this.lastEventId = '';
    this.onEvent = onEvent || null;
    this.onComment = onComment || null;
    this.onRetry = onRetry || null;
    this.onStats = onStats || null;
    this.stats = {
      lines: 0,
      events: 0,
      comments: 0,
      invalidFields: 0,
      invalidRetries: 0,
      bomStripped: 0,
      replacementChars: 0,
    };
    this.resetEvent();
  }

  resetEvent() {
    this._eventType = 'message';
    this._data = [];
    this._eventStartLine = this.stats ? this.stats.lines + 1 : 1;
  }

  // 断流时调用：规范规定解析错误时丢弃未派发的事件，但 lastEventId 保留（用于续传）。
  // 统计数字刻意累计不清零，便于跨会话观测报文质量。
  resetStream() {
    this._lineBuffer = '';
    this._atStart = false;
    this.resetEvent();
  }

  feed(bytes) {
    const now = () => (typeof performance !== 'undefined' ? performance.now() : Date.now());
    const t0 = now();
    let text;
    if (bytes instanceof Uint8Array) {
      text = this._decoder.decode(bytes, { stream: true });
    } else {
      text = String(bytes == null ? '' : bytes);
    }
    if (!this._bomChecked) {
      this._bomChecked = true;
      if (text.charCodeAt(0) === 0xFEFF) {
        text = text.slice(1);
        this.stats.bomStripped++;
      }
    }
    if (text.indexOf('\uFFFD') !== -1) this.stats.replacementChars++;

    let pos = 0;
    while (pos < text.length) {
      const lf = text.indexOf('\n', pos);
      if (lf === -1) break;
      let line;
      if (this._atStart && this._lineBuffer === '') {
        line = text.slice(pos, lf);
        pos = lf + 1;
        this._atStart = false;
      } else {
        this._lineBuffer += text.slice(pos, lf);
        pos = lf + 1;
        line = this._lineBuffer;
        this._lineBuffer = '';
      }
      if (line.charCodeAt(line.length - 1) === 0x0D) line = line.slice(0, -1);
      this.stats.lines++;
      this.handleLine(line);
    }
    if (pos < text.length) this._lineBuffer += text.slice(pos);
    if (this.onStats) this.onStats(this.stats, now() - t0);
    return this.stats;
  }

  handleLine(line) {
    if (line === '') {
      this.dispatch();
      return;
    }
    if (line.charCodeAt(0) === 0x3A) {
      this.stats.comments++;
      if (this.onComment) this.onComment(line.slice(1), this.stats.lines);
      return;
    }
    const colon = line.indexOf(':');
    let field;
    let value;
    if (colon === -1) {
      field = line;
      value = '';
    } else {
      field = line.slice(0, colon);
      value = line.slice(colon + 1);
      if (value.charCodeAt(0) === 0x20) value = value.slice(1);
    }
    switch (field) {
      case 'data':
        this._data.push(value);
        break;
      case 'event':
        this._eventType = value;
        break;
      case 'id':
        // 规范：id 值含 U+0000 时忽略整个字段，不更新 lastEventId
        if (value.indexOf('\u0000') === -1) this.lastEventId = value;
        else this.stats.invalidFields++;
        break;
      case 'retry': {
        if (/^[0-9]+$/.test(value)) {
          if (this.onRetry) this.onRetry(Number(value), this.stats.lines);
        } else {
          // 非法 retry：忽略，保留现有（或默认）重连间隔
          this.stats.invalidRetries++;
          this.stats.invalidFields++;
        }
        break;
      }
      default:
        // 未知字段、空字段名等一律忽略且不崩
        this.stats.invalidFields++;
        break;
    }
  }

  dispatch() {
    // 只有 data 缓冲非空才派发；仅含 event/id、空行结束的报文不产生事件。
    if (this._data.length === 0) {
      this.resetEvent();
      return;
    }
    const ev = {
      event: this._eventType,
      data: this._data.join('\n'),
      id: this.lastEventId,
      startLine: this._eventStartLine,
      endLine: this.stats.lines,
    };
    this.stats.events++;
    if (this.onEvent) this.onEvent(ev);
    this.resetEvent();
  }

  // 脚本正常结束时收尾：模拟“末尾补一个空行”，把无空行结尾的事件派发出去。
  endStream() {
    if (this._lineBuffer !== '') {
      if (this._lineBuffer.charCodeAt(this._lineBuffer.length - 1) === 0x0D) {
        this._lineBuffer = this._lineBuffer.slice(0, -1);
      }
      const line = this._lineBuffer;
      this._lineBuffer = '';
      this.stats.lines++;
      this.handleLine(line);
    }
    this.dispatch();
  }
}

export { DEFAULT_RETRY_MS };
