// SSE 解析 Worker：接收主线程分块传输的 Uint8Array（零拷贝 transfer），
// 在后台线程完成 UTF-8 流式解码、按行切分与 SSE 字段解析，
// 再把每一批的事件/注释/retry 指令回传主线程。
import { SSEParser } from '../lib/parser.js';

let parser = null;
let pending = null;
let batchParseMs = 0;

function newPending() {
  return { events: [], comments: [], retries: [] };
}

function initParser(carriedId = '') {
  pending = newPending();
  batchParseMs = 0;
  parser = new SSEParser({
    onEvent: (ev) => pending.events.push(ev),
    onComment: (text, line) => pending.comments.push({ text, line }),
    onRetry: (ms, line) => pending.retries.push({ ms, line }),
    onStats: (_stats, dt) => { batchParseMs += dt; },
  });
}

self.onmessage = (e) => {
  const msg = e.data;
  switch (msg.type) {
    case 'init':
      initParser();
      parser.lastEventId = String(msg.lastEventId || '');
      self.postMessage({ type: 'ready', lastEventId: parser.lastEventId, stats: parser.stats });
      break;
    case 'feed': {
      let bytes = msg.bytes;
      if (bytes instanceof ArrayBuffer) bytes = new Uint8Array(bytes);
      parser.feed(bytes);
      const batch = pending;
      pending = newPending();
      self.postMessage({
        type: 'batch',
        seq: msg.seq,
        events: batch.events,
        comments: batch.comments,
        retries: batch.retries,
        lastEventId: parser.lastEventId,
        stats: parser.stats,
        parseMs: batchParseMs,
      });
      batchParseMs = 0;
      break;
    }
    case 'reset':
      // 断流：丢弃未派发事件，lastEventId 在解析器内保留
      parser.resetStream();
      self.postMessage({ type: 'reset-done', seq: msg.seq, lastEventId: parser.lastEventId });
      break;
    case 'end':
      parser.endStream();
      self.postMessage({
        type: 'ended',
        seq: msg.seq,
        events: pending.events,
        comments: pending.comments,
        retries: pending.retries,
        lastEventId: parser.lastEventId,
        stats: parser.stats,
        parseMs: batchParseMs,
      });
      pending = newPending();
      break;
    default:
      break;
  }
};
