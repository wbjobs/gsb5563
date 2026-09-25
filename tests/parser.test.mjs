import { SSEParser, DEFAULT_RETRY_MS } from '../lib/parser.js';

let passed = 0;
let failed = 0;
function assert(cond, name, extra) {
  if (cond) { passed++; }
  else { failed++; console.error('FAIL:', name, extra ?? ''); }
}
function parseAll(chunks, opts = {}) {
  const events = [];
  const comments = [];
  const retries = [];
  const p = new SSEParser({
    onEvent: (e) => events.push(e),
    onComment: (c) => comments.push(c),
    onRetry: (ms) => retries.push(ms),
    ...opts,
  });
  for (const c of chunks) p.feed(c);
  return { p, events, comments, retries };
}
const enc = (s) => new TextEncoder().encode(s);

// 1. 基本字段解析
{
  const { events } = parseAll([enc('event: update\ndata: hello\nid: 42\n\n')]);
  assert(events.length === 1, 'basic: one event');
  assert(events[0]?.event === 'update', 'basic: event field');
  assert(events[0]?.data === 'hello', 'basic: data field');
  assert(events[0]?.id === '42', 'basic: id field');
}

// 2. 默认 event 类型 & 冒号后只去一个空格
{
  const { events } = parseAll([enc('data:  two spaces\n\n')]);
  assert(events[0].event === 'message', 'default event type');
  assert(events[0].data === ' two spaces', 'only one leading space stripped');
}

// 3. 多行 data 以 \n 拼接
{
  const { events } = parseAll([enc('data: line1\ndata:line2\ndata: line3\n\n')]);
  assert(events[0].data === 'line1\nline2\nline3', 'multi-line data join');
}

// 4. 注释行被忽略且计数
{
  const { events, comments, p } = parseAll([enc(': a comment\n:no-space-comment\ndata: x\n\n')]);
  assert(events.length === 1, 'comment does not produce event');
  assert(comments.length === 2, 'comment captured');
  assert(comments[0] === ' a comment' && comments[1] === 'no-space-comment', 'comment text');
  assert(p.stats.comments === 2, 'comment stats');
}

// 5. 非法字段被忽略不崩
{
  const { events, p } = parseAll([enc('bogus: value\nfoo\ndata: ok\n\n')]);
  assert(events.length === 1 && events[0].data === 'ok', 'unknown field ignored');
  assert(p.stats.invalidFields >= 2, 'invalid fields counted');
}

// 6. 非法 retry 被忽略
{
  const { retries, p } = parseAll([enc('retry: 1500\nretry: abc\nretry: 12px\nretry:\n\n')]);
  assert(retries.length === 1 && retries[0] === 1500, 'only valid retry accepted');
  assert(p.stats.invalidRetries === 3, 'invalid retry counted');
}

// 7. 无 data 的事件不派发，但 id 仍更新
{
  const { events, p } = parseAll([enc('id: 99\nevent: ping\n\n')]);
  assert(events.length === 0, 'event without data not dispatched');
  assert(p.lastEventId === '99', 'lastEventId still updated');
}

// 8. id 含 NUL 被忽略
{
  const { p } = parseAll([enc('id: 7\nid: bad\u0000id\ndata: x\n\n')]);
  assert(p.lastEventId === '7', 'NUL id ignored, previous id retained');
}

// 9. 断流：未派发事件被丢弃，lastEventId 保留
{
  const { p } = parseAll([enc('id: 123\ndata: partial no blank line')]);
  p.resetStream();
  assert(p.lastEventId === '123', 'lastEventId survives disconnect');
  const more = [];
  p.onEvent = (e) => more.push(e);
  p.feed(enc('data: after-reconnect\n\n'));
  assert(more.length === 1 && more[0].data === 'after-reconnect', 'parser resumes after resetStream');
  assert(more[0].id === '123', 'resumed event carries Last-Event-ID value');
}

// 10. 跨 chunk 任意切分（含多字节 UTF-8 被切开）
{
  const full = 'id: 1\ndata: 你好，世界 🎉 line1\ndata: line2\nevent: chat\n\n';
  const sizes = [1, 2, 3, 5, 7];
  for (const size of sizes) {
    const bytes = enc(full);
    const chunks = [];
    for (let i = 0; i < bytes.length; i += size) chunks.push(bytes.slice(i, i + size));
    const { events } = parseAll(chunks);
    assert(events.length === 1, `chunk size ${size}: one event`);
    assert(events[0]?.data === '你好，世界 🎉 line1\nline2', `chunk size ${size}: multibyte + multiline`, events[0]?.data);
    assert(events[0]?.id === '1' && events[0]?.event === 'chat', `chunk size ${size}: fields`);
  }
}

// 11. CRLF / 裸 CR 不残留
{
  const { events } = parseAll([enc('data: crlf\r\n\r\n')]);
  assert(events[0].data === 'crlf', 'CRLF handled');
}

// 12. 前导 BOM 只去掉一次
{
  const bytes = new Uint8Array([0xEF, 0xBB, 0xBF, ...enc('data: bom\n\n')]);
  const { events, p } = parseAll([bytes]);
  assert(events[0].data === 'bom', 'BOM stripped');
  assert(p.stats.bomStripped === 1, 'bom stat');
}

// 13. 空 data 行派发空字符串事件
{
  const { events } = parseAll([enc('data:\n\n')]);
  assert(events.length === 1 && events[0].data === '', 'empty data dispatches empty-string event');
}

// 14. 连续空行 / 多事件顺序
{
  const { events } = parseAll([enc('data: a\n\ndata: b\n\n\n')]);
  assert(events.length === 2 && events[0].data === 'a' && events[1].data === 'b', 'multiple events in order');
}

// 15. endStream 收尾无空行结尾的事件
{
  const { events } = parseAll([enc('id: 5\ndata: tail')]);
  // parseAll 不自动 endStream
  assert(events.length === 0, 'no dispatch before EOF flush');
}

// 16. 一万条事件 & 随机乱序切分不卡（计时）
{
  const parts = [];
  for (let i = 0; i < 10000; i++) parts.push(`id: ${i}\nevent: tick\ndata: payload-${i}\n\n`);
  const bytes = enc(parts.join(''));
  const events = [];
  const p = new SSEParser({ onEvent: (e) => events.push(e) });
  const t0 = Date.now();
  // 模拟 chaos：随机 1~64 字节切分
  let i = 0;
  while (i < bytes.length) {
    const n = 1 + Math.floor(Math.random() * 64);
    p.feed(bytes.slice(i, i + n));
    i += n;
  }
  const ms = Date.now() - t0;
  assert(events.length === 10000, '10k events parsed');
  assert(events[0].id === '0' && events[9999].id === '9999', '10k ids in order');
  assert(events[9999].data === 'payload-9999', '10k last payload');
  console.log(`10k random-chunk parse: ${ms}ms`);
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
