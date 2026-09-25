// Canvas 时序可视化：泳道展示会话区间，主轨道绘制事件/心跳/连接/重连/去重等标记。
// 支持滚轮以光标为锚点缩放、拖拽平移、点击选中、悬停 tooltip、自动跟随最新事件。
const COLORS = {
  event: '#4da3ff',
  heartbeat: '#8a93a6',
  connecting: '#e0b341',
  connected: '#3ecf8e',
  reconnect: '#f0883e',
  duplicate: '#c066ff',
  retry: '#f0883e',
  invalid: '#ff6b6b',
  note: '#6b7280',
  silent: '#7c8699',
  expectok: '#3ecf8e',
  fail: '#ff6b6b',
  done: '#3ecf8e',
  closed: '#ff6b6b',
};

const MARK_KINDS = new Map([
  ['connecting', { color: COLORS.connecting, label: '建立连接' }],
  ['connected', { color: COLORS.connected, label: '连接已建立' }],
  ['reconnect-attempt', { color: COLORS.reconnect, label: '重连(携带 Last-Event-ID)' }],
  ['reconnect-wait', { color: COLORS.reconnect, label: '退避等待' }],
  ['heartbeat', { color: COLORS.heartbeat, label: '心跳' }],
  ['duplicate', { color: COLORS.duplicate, label: '重复 ID 已去重' }],
  ['invalid-retry', { color: COLORS.invalid, label: '非法 retry 已忽略' }],
  ['dropped-note', { color: COLORS.note, label: '断流丢弃未完成事件' }],
  ['server-silent', { color: COLORS.silent, label: '服务端静默' }],
  ['expect-id-ok', { color: COLORS.expectok, label: 'Last-Event-ID 校验通过' }],
  ['expect-id-fail', { color: COLORS.fail, label: 'Last-Event-ID 不匹配' }],
  ['done', { color: COLORS.done, label: '模拟结束' }],
  ['closed', { color: COLORS.closed, label: '已关闭' }],
]);

export class TimelineView {
  constructor(canvas, tooltipEl, timeline) {
    this.canvas = canvas;
    this.ctx = canvas.getContext('2d');
    this.tooltip = tooltipEl;
    this.timeline = timeline;
    this.viewStart = null;
    this.viewEnd = null;
    this.follow = true;
    this._drag = null;
    this._hover = null;
    this._selectedSeq = null;
    this._onSelect = null;
    this._bind();
    this.resize();
    requestAnimationFrame(() => this.ensureWindow());
  }

  onSelect(fn) { this._onSelect = fn; }

  _bind() {
    const ro = new ResizeObserver(() => { this.resize(); this.draw(); });
    ro.observe(this.canvas);
    this.canvas.addEventListener('wheel', (e) => {
      e.preventDefault();
      this.ensureWindow();
      const rect = this.canvas.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const span = this.viewEnd - this.viewStart;
      const tAt = this.viewStart + (x / this.w) * span;
      const factor = e.deltaY > 0 ? 1.2 : 1 / 1.2;
      let ns = span * factor;
      ns = Math.max(50, Math.min(ns, 24 * 3600 * 1000));
      this.viewStart = tAt - (x / this.w) * ns;
      this.viewEnd = this.viewStart + ns;
      this.follow = false;
      this.draw();
    }, { passive: false });
    this.canvas.addEventListener('mousedown', (e) => {
      this._drag = { x: e.clientX, vs: this.viewStart, ve: this.viewEnd };
      this.canvas.style.cursor = 'grabbing';
    });
    window.addEventListener('mousemove', (e) => this._onMove(e));
    window.addEventListener('mouseup', () => {
      if (this._drag) {
        this._drag = null;
        this.canvas.style.cursor = '';
      }
    });
    this.canvas.addEventListener('click', (e) => {
      const hit = this._hitTest(e);
      if (hit && hit.kind === 'event' && this._onSelect) this._onSelect(hit.seq);
    });
  }

  _onMove(e) {
    if (this._drag) {
      const rect = this.canvas.getBoundingClientRect();
      const span = this._drag.ve - this._drag.vs;
      const dx = e.clientX - this._drag.x;
      const shift = (dx / this.w) * span;
      this.viewStart = this._drag.vs - shift;
      this.viewEnd = this._drag.ve - shift;
      this.follow = false;
      this.draw();
      return;
    }
    const hit = this._hitTest(e);
    this._hover = hit;
    this.canvas.style.cursor = hit && hit.kind === 'event' ? 'pointer' : '';
    this.draw();
    this._renderTooltip(e, hit);
  }

  _hitTest(e) {
    const rect = this.canvas.getBoundingClientRect();
    if (e.clientX < rect.left || e.clientX > rect.right || e.clientY < rect.top || e.clientY > rect.bottom) return null;
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    this.ensureWindow();
    const laneY = this._laneY('main');
    for (let i = this.timeline.entries.length - 1; i >= 0; i--) {
      const it = this.timeline.entries[i];
      if (it.ts < this.viewStart || it.ts > this.viewEnd) continue;
      const px = this._x(it.ts);
      if (Math.abs(px - x) <= 4 && Math.abs(y - laneY) <= 9) return it;
    }
    return null;
  }

  _renderTooltip(e, hit) {
    if (!hit) { this.tooltip.style.display = 'none'; return; }
    let html;
    if (hit.kind === 'event') {
      const data = hit.data.length > 120 ? hit.data.slice(0, 120) + '…' : hit.data;
      html = `<b>事件</b> id=<code>${escapeHtml(hit.id || '(空)')}</code> type=<code>${escapeHtml(hit.eventType)}</code><br>${escapeHtml(data).replace(/\n/g, '<br>')}`;
    } else {
      const meta = MARK_KINDS.get(hit.kind);
      html = `<b>${escapeHtml(meta ? meta.label : hit.kind)}</b>` + (hit.detail ? `<br>${escapeHtml(hit.detail)}` : '');
    }
    this.tooltip.innerHTML = html;
    this.tooltip.style.display = 'block';
    const rect = this.canvas.getBoundingClientRect();
    let left = e.clientX - rect.left + 12;
    let top = e.clientY - rect.top + 12;
    if (left + 220 > this.w) left -= 230;
    this.tooltip.style.left = left + 'px';
    this.tooltip.style.top = top + 'px';
  }

  resize() {
    const dpr = window.devicePixelRatio || 1;
    const rect = this.canvas.getBoundingClientRect();
    this.w = rect.width;
    this.h = rect.height;
    this.canvas.width = Math.round(rect.width * dpr);
    this.canvas.height = Math.round(rect.height * dpr);
    this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  ensureWindow() {
    if (this.viewStart === null) {
      this.viewEnd = performance.now() + 1000;
      this.viewStart = this.viewEnd - 15000;
    }
    if (this.follow) {
      const now = performance.now();
      const last = this.timeline.entries.length
        ? this.timeline.entries[this.timeline.entries.length - 1].ts : now;
      const span = this.viewEnd - this.viewStart;
      this.viewEnd = Math.max(now, last) + 200;
      this.viewStart = this.viewEnd - span;
    }
  }

  setFollow(v) { this.follow = v; if (v) this.draw(); }

  select(seq) { this._selectedSeq = seq; this.draw(); }

  _x(ts) { return ((ts - this.viewStart) / (this.viewEnd - this.viewStart)) * this.w; }

  _laneY(name) {
    return name === 'main' ? this.h - 64 : 26;
  }

  draw() {
    this.ensureWindow();
    const c = this.ctx;
    c.clearRect(0, 0, this.w, this.h);
    this._drawGrid();
    this._drawSessions();
    this._drawEntries();
    this._drawAxis();
  }

  _drawGrid() {
    const c = this.ctx;
    c.strokeStyle = 'rgba(255,255,255,0.06)';
    c.lineWidth = 1;
    const span = this.viewEnd - this.viewStart;
    const step = niceStep(span / 8);
    const t0 = Math.ceil(this.viewStart / step) * step;
    c.fillStyle = '#7c8699';
    c.font = '10px monospace';
    for (let t = t0; t < this.viewEnd; t += step) {
      const x = this._x(t);
      c.beginPath(); c.moveTo(x, 0); c.lineTo(x, this.h - 40); c.stroke();
      c.fillText(fmtRel(t, this.viewStart), x + 3, this.h - 44);
    }
  }

  _drawSessions() {
    const c = this.ctx;
    const y = this._laneY('main') + 22;
    c.font = '10px monospace';
    for (const sp of this.timeline.sessions) {
      if (sp.openTs == null) continue;
      const x0 = this._x(Math.max(sp.openTs, this.viewStart));
      const x1 = this._x(Math.min(sp.closeTs || performance.now(), this.viewEnd));
      if (x1 < 0 || x0 > this.w) continue;
      const hue = sp.session * 47;
      c.fillStyle = `hsla(${hue % 360},70%,60%,0.12)`;
      c.fillRect(x0, 8, Math.max(2, x1 - x0), this.h - 70);
      c.fillStyle = `hsla(${hue % 360},80%,75%,0.8)`;
      c.fillText(`会话 ${sp.session}${sp.outcome ? ' · ' + (sp.outcome === 'complete' ? '正常结束' : '断线') : ''}`, x0 + 4, 20);
    }
  }

  _drawEntries() {
    const c = this.ctx;
    const y = this._laneY('main');
    let lastPx = -Infinity;
    let stack = 0;
    for (const it of this.timeline.entries) {
      if (it.ts < this.viewStart - 50 || it.ts > this.viewEnd + 50) continue;
      const px = this._x(it.ts);
      if (it.kind === 'event') {
        if (px - lastPx < 1.5) { stack++; } else { stack = 0; }
        lastPx = px;
        const yy = y - (stack % 6) * 2;
        c.fillStyle = COLORS.event;
        if (this._selectedSeq === it.seq) {
          c.strokeStyle = '#fff';
          c.beginPath(); c.arc(px, yy, 4.5, 0, Math.PI * 2); c.stroke();
        }
        c.fillRect(px - 1, yy - 4, 2, 8);
      } else {
        const meta = MARK_KINDS.get(it.kind);
        if (!meta) continue;
        this._drawMarker(px, y, meta.color, it);
      }
    }
  }

  _drawMarker(px, y, color, item) {
    const c = this.ctx;
    const span = this.viewEnd - this.viewStart;
    const showLabel = span < 120000;
    c.fillStyle = color;
    c.beginPath();
    if (item.kind === 'heartbeat' || item.kind === 'server-silent') {
      c.arc(px, y, 2.5, 0, Math.PI * 2);
    } else {
      c.moveTo(px, y - 6); c.lineTo(px + 5, y); c.lineTo(px, y + 6); c.lineTo(px - 5, y);
    }
    c.fill();
    if (showLabel && item.label) {
      c.font = '9px sans-serif';
      c.fillText(item.label, px + 6, y - 7);
    }
  }

  _drawAxis() {
    const c = this.ctx;
    const y = this._laneY('main');
    c.strokeStyle = 'rgba(255,255,255,0.25)';
    c.beginPath(); c.moveTo(0, y); c.lineTo(this.w, y); c.stroke();
    c.fillStyle = '#7c8699';
    c.font = '10px monospace';
    c.fillText('事件流时间轴（相对开始时间，单位 ms）', 8, this.h - 14);
  }
}

function niceStep(raw) {
  const pow = 10 ** Math.floor(Math.log10(raw));
  const n = raw / pow;
  const f = n < 1.5 ? 1 : n < 3.5 ? 2 : n < 7.5 ? 5 : 10;
  return f * pow;
}

function fmtRel(t, start) {
  return Math.round(t - start) + 'ms';
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (ch) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[ch]));
}
