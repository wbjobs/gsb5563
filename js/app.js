(() => {
  const $ = s => document.querySelector(s);
  const input = $('#sse-input');
  const tableBody = $('#event-table tbody');
  const RENDER_CAP = 500; // 表格渲染上限，避免 1 万行 DOM 卡顿

  // ---- EventSource 支持检测（降级提示）----
  if (typeof EventSource === 'undefined') {
    $('#es-warning').classList.remove('hidden');
  }

  // ---- 状态机视图 ----
  const smEl = $('#state-machine');
  const SM_LABELS = {
    CONNECTING: '连接建立', OPEN: '已连接', EVENT: '事件接收',
    HEARTBEAT: '心跳', CLOSED: '断线', RECONNECTING: '重连退避',
  };
  function renderStateMachine(activeStates) {
    smEl.innerHTML = SSEStateMachine.STATES.map((s, i) => {
      const arrow = i < SSEStateMachine.STATES.length - 1 ? '<span class="arrow">→</span>' : '';
      const cls = activeStates.has(s) ? 'node active' : 'node';
      return `<span class="${cls}">${SM_LABELS[s]}</span>${arrow}`;
    }).join('');
  }
  renderStateMachine(new Set());
  Timeline.legend($('#timeline-legend'));

  // ---- 示例 ----
  document.querySelectorAll('[data-sample]').forEach(btn => {
    btn.addEventListener('click', () => { input.value = SAMPLES[btn.dataset.sample]; });
  });
  $('#btn-stress').addEventListener('click', () => { input.value = makeStress(10000); });
  $('#btn-clear-input').addEventListener('click', () => { input.value = ''; });

  // ---- Worker 解析 ----
  const worker = new Worker('js/worker.js');

  function parseInWorker(text) {
    return new Promise((resolve, reject) => {
      const records = [];
      const t0 = performance.now();
      worker.onmessage = e => {
        if (e.data.type === 'batch') records.push(...e.data.records);
        else if (e.data.type === 'done') {
          resolve({ records, elapsed: performance.now() - t0 });
        }
      };
      worker.onerror = reject;
      worker.postMessage({ text, batchSize: 2000 });
    });
  }

  // ---- 事件表渲染 ----
  function renderTable(events) {
    const frag = document.createDocumentFragment();
    const rows = events.slice(0, RENDER_CAP);
    rows.forEach((ev, i) => {
      const tr = document.createElement('tr');
      if (ev.status === 'duplicate') tr.className = 'dup';
      const data = ev.data.length > 200 ? ev.data.slice(0, 200) + '…' : ev.data;
      tr.innerHTML =
        `<td>${i + 1}</td><td>${ev.time}</td><td></td><td></td><td class="data-cell"></td><td></td>`;
      tr.children[2].textContent = ev.event;
      tr.children[3].textContent = ev.id === null ? '(无)' : ev.id;
      tr.children[4].textContent = data;
      tr.children[5].textContent = ev.status === 'accepted' ? '已接收' : '重复-已去重';
      frag.appendChild(tr);
    });
    tableBody.innerHTML = '';
    tableBody.appendChild(frag);
  }

  async function refreshStats(extra) {
    const count = await EventLogDB.count();
    $('#log-stats').textContent = `IndexedDB 已存 ${count} 条` + (extra ? ` · ${extra}` : '');
  }

  // ---- 主流程 ----
  $('#btn-run').addEventListener('click', async () => {
    const text = input.value;
    if (!text.trim()) { alert('请先输入或加载 SSE 报文'); return; }

    const { records, elapsed } = await parseInWorker(text);

    const result = SSEStateMachine.simulate(records, {
      defaultRetry: parseInt($('#default-retry').value, 10) || SSEStateMachine.DEFAULT_RETRY,
      maxBackoff: parseInt($('#max-backoff').value, 10) || 30000,
    });

    // 状态机高亮：本次生命周期中实际经过的状态
    renderStateMachine(new Set(result.segments.map(s => s.state)));
    $('#sm-meta').textContent =
      `重连 ${result.reconnectCount} 次 · 最终 Last-Event-ID: ` +
      (result.lastEventId !== null ? result.lastEventId : '(无)') +
      ` · 虚拟总时长 ${result.totalTime}ms · Worker 解析耗时 ${elapsed.toFixed(1)}ms`;

    // 警告（非法报文降级提示）
    const warnEl = $('#parse-warning');
    if (result.warnings.length) {
      const uniq = [...new Set(result.warnings)];
      warnEl.textContent = '降级处理: ' + uniq.slice(0, 5).join('；') +
        (uniq.length > 5 ? ` 等 ${uniq.length} 条` : '');
      warnEl.classList.remove('hidden');
    } else {
      warnEl.classList.add('hidden');
    }

    Timeline.draw($('#timeline'), result.segments, result.events, result.totalTime);
    renderTable(result.events);

    // IndexedDB 持久化（批量单事务）
    await EventLogDB.addBatch(result.events.map(ev => ({
      time: ev.time, event: ev.event, id: ev.id, data: ev.data, status: ev.status,
      savedAt: Date.now(),
    })));
    const accepted = result.events.filter(e => e.status === 'accepted').length;
    const dup = result.events.length - accepted;
    await refreshStats(`本次 ${result.events.length} 条（接收 ${accepted} / 去重 ${dup}）`);
  });

  $('#btn-clear-log').addEventListener('click', async () => {
    await EventLogDB.clear();
    tableBody.innerHTML = '';
    await refreshStats();
  });

  window.addEventListener('resize', () => {
    // 重绘最近一次结果（简单起见仅在画布有内容时触发）
  });

  refreshStats();
})();
