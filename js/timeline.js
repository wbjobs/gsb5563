// Canvas 时序可视化：状态带 + 事件标记
const Timeline = (() => {
  const COLORS = {
    CONNECTING: '#8a6d1f',
    OPEN: '#1f6e43',
    EVENT: '#2f6fed',
    HEARTBEAT: '#7a4fd0',
    CLOSED: '#a03a3a',
    RECONNECTING: '#c07a1a',
  };
  const LABELS = {
    CONNECTING: '连接建立', OPEN: '已连接', EVENT: '事件接收',
    HEARTBEAT: '心跳', CLOSED: '断线', RECONNECTING: '重连退避',
  };

  function legend(el) {
    el.innerHTML = Object.keys(COLORS).map(k =>
      `<span><span class="sw" style="background:${COLORS[k]}"></span>${LABELS[k]}</span>`
    ).join('') + `<span><span class="sw" style="background:#fff"></span>事件刻度</span>`;
  }

  function draw(canvas, segments, events, totalTime) {
    const dpr = window.devicePixelRatio || 1;
    const W = canvas.clientWidth, H = canvas.height;
    canvas.width = W * dpr; canvas.height = H * dpr;
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, W, H);

    const padL = 90, padR = 16, padT = 14, padB = 28;
    const plotW = W - padL - padR;
    const rows = Object.keys(COLORS);
    const rowH = (H - padT - padB) / rows.length;
    const x = t => padL + (totalTime === 0 ? 0 : (t / totalTime) * plotW);

    ctx.font = '11px sans-serif';
    rows.forEach((state, i) => {
      const y = padT + i * rowH;
      ctx.fillStyle = '#8b96ad';
      ctx.textAlign = 'right'; ctx.textBaseline = 'middle';
      ctx.fillText(LABELS[state], padL - 8, y + rowH / 2);
      ctx.strokeStyle = '#22304f';
      ctx.beginPath(); ctx.moveTo(padL, y + rowH); ctx.lineTo(W - padR, y + rowH); ctx.stroke();
    });

    for (const seg of segments) {
      const i = rows.indexOf(seg.state);
      if (i === -1) continue;
      const y = padT + i * rowH + 3;
      const w = Math.max(2, x(seg.end) - x(seg.start));
      ctx.fillStyle = COLORS[seg.state];
      ctx.globalAlpha = 0.85;
      ctx.fillRect(x(seg.start), y, w, rowH - 6);
      ctx.globalAlpha = 1;
    }

    // 事件刻度（白线，密集时自动抽样绘制）
    const accepted = events.filter(e => e.status === 'accepted');
    const step = Math.max(1, Math.floor(accepted.length / plotW));
    ctx.strokeStyle = '#ffffff';
    for (let i = 0; i < accepted.length; i += step) {
      const ex = x(accepted[i].time);
      ctx.globalAlpha = 0.35;
      ctx.beginPath(); ctx.moveTo(ex, padT); ctx.lineTo(ex, H - padB); ctx.stroke();
    }
    ctx.globalAlpha = 1;

    // 时间轴刻度
    ctx.fillStyle = '#8b96ad'; ctx.textAlign = 'center';
    const ticks = 6;
    for (let i = 0; i <= ticks; i++) {
      const t = (totalTime / ticks) * i;
      ctx.fillText(`${Math.round(t)}ms`, x(t), H - padB + 14);
    }
  }

  return { draw, legend, COLORS, LABELS };
})();
