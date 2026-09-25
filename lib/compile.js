// 把用户输入的 SSE 原始报文编译为模拟时间线动作。
// 普通行原样作为服务端流出的字节；以注释形式承载的“模拟指令”驱动服务端行为：
//   :!delay 1200        服务端静默 1200ms（用于观察心跳超时）
//   :!disconnect        服务端中途断流（未派发的事件应被丢弃）
//   :!expect-id <id>    校验重连请求携带的 Last-Event-ID 是否为 <id>
// 输出 actions: [{ type:'feed', session, text } | { type:'delay', ms }
//                | { type:'disconnect', session, line } | { type:'expect-id', value, session, line }]
export function compileScript(rawText) {
  const actions = [];
  const lines = rawText.split('\n');
  let session = 0;
  let pending = '';
  let pendingStartLine = 1;
  let lineNo = 0;

  const flushFeed = () => {
    if (pending) {
      actions.push({ type: 'feed', session, text: pending, startLine: pendingStartLine });
      pending = '';
    }
  };

  for (const rawLine of lines) {
    lineNo++;
    const line = rawLine.endsWith('\r') ? rawLine.slice(0, -1) : rawLine;
    const dm = /^:!delay\s+([0-9]+)\s*$/.exec(line);
    const dc = /^:!disconnect\s*$/.test(line);
    const ex = /^:!expect-id\s+(.*)$/.exec(line);
    if (dm) {
      flushFeed();
      actions.push({ type: 'delay', ms: Number(dm[1]), line: lineNo, session });
    } else if (dc) {
      flushFeed();
      actions.push({ type: 'disconnect', session, line: lineNo });
      session++;
      pendingStartLine = lineNo + 1;
    } else if (ex) {
      flushFeed();
      actions.push({ type: 'expect-id', value: ex[1], session, line: lineNo });
    } else {
      if (!pending) pendingStartLine = lineNo;
      pending += line + '\n';
    }
  }
  flushFeed();
  return { actions, sessions: session + 1 };
}
