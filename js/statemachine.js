// 事件流生命周期状态机（虚拟时间，不占用真实等待）
// CONNECTING -> OPEN -> (EVENT|HEARTBEAT)* -> CLOSED -> RECONNECTING --(Last-Event-ID)--> CONNECTING ...
const SSEStateMachine = (() => {
  const STATES = ['CONNECTING', 'OPEN', 'EVENT', 'HEARTBEAT', 'CLOSED', 'RECONNECTING'];
  const DEFAULT_RETRY = 3000;

  // records: worker 解析结果；options: {defaultRetry, maxBackoff}
  function simulate(records, options) {
    const defaultRetry = options.defaultRetry || DEFAULT_RETRY;
    const maxBackoff = options.maxBackoff || 30000;

    let retry = defaultRetry;          // 服务端可动态更新
    let lastEventId = null;            // Last-Event-ID
    const seenIds = new Set();
    let time = 0;
    let reconnectCount = 0;

    const segments = [];   // {state, start, end, note}
    const events = [];     // 接收的事件（含去重标记）
    const warnings = [];

    function enter(state, note) {
      segments.push({ state, start: time, end: time, note: note || '' });
    }
    function leave() {
      segments[segments.length - 1].end = time;
    }
    function tick(ms) { time += ms; }

    enter('CONNECTING', '发起连接');
    tick(50);
    leave(); enter('OPEN', '连接建立 (HTTP 200, text/event-stream)');

    for (const rec of records) {
      switch (rec.kind) {
        case 'retry':
          retry = rec.value;
          break;
        case 'invalid-retry':
          retry = defaultRetry;
          warnings.push(`非法 retry "${rec.value}"，降级为默认 ${defaultRetry}ms`);
          break;
        case 'ignored-field':
          warnings.push(`忽略未识别字段: ${rec.field}`);
          break;
        case 'malformed':
          warnings.push(`忽略非法行（无冒号）: ${rec.line}`);
          break;
        case 'comment':
          leave(); enter('HEARTBEAT', rec.text ? `心跳: ${rec.text}` : '心跳（注释行）');
          tick(5); leave(); enter('OPEN', '');
          break;
        case 'event': {
          leave(); enter('EVENT', `event=${rec.event}`);
          tick(5);
          const isDup = rec.id !== null && seenIds.has(rec.id);
          if (!isDup) {
            if (rec.id !== null) { seenIds.add(rec.id); lastEventId = rec.id; }
            events.push({ time, event: rec.event, id: rec.id, data: rec.data, status: 'accepted' });
          } else {
            events.push({ time, event: rec.event, id: rec.id, data: rec.data, status: 'duplicate' });
          }
          leave(); enter('OPEN', '');
          break;
        }
        case 'directive':
          if (rec.name === 'disconnect') {
            leave(); enter('CLOSED', '服务端断流');
            tick(20);
            leave(); enter('RECONNECTING',
              `按 retry=${retry}ms 退避重连` + (lastEventId !== null ? `，携带 Last-Event-ID: ${lastEventId}` : ''));
            tick(Math.min(retry, maxBackoff));
            reconnectCount++;
            leave(); enter('CONNECTING',
              lastEventId !== null ? `重连请求头 Last-Event-ID: ${lastEventId}` : '重连（无 Last-Event-ID）');
            tick(50);
            leave(); enter('OPEN', '重连成功，续传事件流');
          }
          break;
        case 'trailing-buffer-discarded':
          warnings.push('流末尾存在未以空行结束的事件缓冲，按规范丢弃');
          break;
      }
    }
    leave();
    segments.push({ state: 'CLOSED', start: time, end: time + 20, note: '流结束' });
    time += 20;

    return { segments, events, warnings, totalTime: time, lastEventId, reconnectCount };
  }

  return { STATES, simulate, DEFAULT_RETRY };
})();
