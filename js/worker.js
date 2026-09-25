// SSE 解析 Worker：TextEncoder -> Uint8Array 字节级解析，避免阻塞主线程。
// 输出记录类型：
//  {kind:'event', event, data, id, retry, rawLines}
//  {kind:'comment', text}            心跳/注释行
//  {kind:'ignored-field', field}     未识别字段（按规范忽略）
//  {kind:'invalid-retry', value}     retry 非数字，降级
//  {kind:'directive', name}          @disconnect 等模拟指令
//  {kind:'malformed', line}          无冒号且非指令的行（按规范忽略）

const LF = 10, CR = 13, COLON = 58, SPACE = 32;

function parseSSE(text) {
  const bytes = new TextEncoder().encode(text); // TypedArray
  const decoder = new TextDecoder();
  const records = [];
  let lineStart = 0;

  // 当前事件缓冲
  let dataBuf = [];
  let eventType = '';
  let lastId = null;      // null 表示本报文块未设置 id
  let pendingRetry = null;
  let rawLines = [];
  let hasData = false;

  function resetEvent() {
    dataBuf = []; eventType = ''; lastId = null; pendingRetry = null; rawLines = []; hasData = false;
  }

  function dispatch() {
    // 规范：空行触发 dispatch；无 data 则丢弃事件（但 retry 仍生效）
    if (pendingRetry !== null) {
      records.push({ kind: 'retry', value: pendingRetry });
    }
    if (hasData) {
      records.push({
        kind: 'event',
        event: eventType || 'message',
        data: dataBuf.join('\n'),
        id: lastId,
        rawLines: rawLines.slice(),
      });
    }
    resetEvent();
  }

  function processLine(line) {
    if (line === '') { dispatch(); return; }
    if (line.startsWith(':')) { // 注释行 = 心跳
      records.push({ kind: 'comment', text: line.slice(1).trim() });
      return;
    }
    if (line.startsWith('@')) { // 本地模拟指令
      records.push({ kind: 'directive', name: line.slice(1).trim() });
      return;
    }
    const colon = line.indexOf(':');
    let field, value;
    if (colon === -1) { // 规范：整行作为字段名，值为空；未知字段被忽略
      field = line; value = '';
      records.push({ kind: 'malformed', line });
    } else {
      field = line.slice(0, colon);
      value = line.slice(colon + 1);
      if (value.startsWith(' ')) value = value.slice(1);
    }
    rawLines.push(line);
    switch (field) {
      case 'data':
        dataBuf.push(value); hasData = true;
        break;
      case 'event':
        eventType = value;
        break;
      case 'id':
        if (!value.includes('\u0000')) lastId = value; // 含 NUL 则忽略（规范要求）
        break;
      case 'retry':
        if (/^\d+$/.test(value)) {
          pendingRetry = parseInt(value, 10);
        } else {
          records.push({ kind: 'invalid-retry', value });
        }
        break;
      default:
        records.push({ kind: 'ignored-field', field });
    }
  }

  // 字节扫描，兼容 \n / \r\n / \r
  let i = 0;
  while (i < bytes.length) {
    const b = bytes[i];
    if (b === LF || b === CR) {
      processLine(decoder.decode(bytes.subarray(lineStart, i)));
      if (b === CR && i + 1 < bytes.length && bytes[i + 1] === LF) i++;
      i++;
      lineStart = i;
    } else {
      i++;
    }
  }
  if (lineStart < bytes.length) {
    processLine(decoder.decode(bytes.subarray(lineStart)));
  }
  // 流末尾未分发的事件缓冲：按规范在连接关闭时丢弃（此处显式标记）
  if (hasData || pendingRetry !== null) {
    records.push({ kind: 'trailing-buffer-discarded' });
  }
  return records;
}

self.onmessage = (e) => {
  const { text, batchSize } = e.data;
  const records = parseSSE(text);
  const size = batchSize || 2000;
  for (let i = 0; i < records.length; i += size) {
    self.postMessage({ type: 'batch', records: records.slice(i, i + size) });
  }
  self.postMessage({ type: 'done', total: records.length });
};
