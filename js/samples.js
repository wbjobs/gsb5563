// 内置示例报文：覆盖所有异常/边界场景；压测脚本生成 N 条事件。
export function demoScript() {
  return [
    ': 欢迎使用 SSE Lab，本行是注释（心跳前的说明）',
    'retry: 800',
    'event: hello',
    'id: evt-1',
    'data: {"msg":"连接建立后的第一条事件"}',
    '',
    ': 心跳行（规范中 SSE 注释常用于 keep-alive）',
    '',
    'data: 心跳之后的第一条事件',
    '',
    'id: evt-2',
    'data: 单行事件，只有一个空格会被吃掉',
    '',
    'id: evt-3',
    'data: 这是第一行',
    'data:这是第二行(冒号后无空格)',
    'data:  第三行保留一个前导空格',
    '',
    'bogusfield: 未知字段应被忽略',
    'noColonLine',
    'data: 非法字段之后仍能正常解析',
    '',
    'retry: not-a-number',
    'retry: 12.5ms',
    'data: 上面两个非法 retry 被忽略，继续使用 800ms',
    '',
    'id: bad\u0000id',
    'data: id 含 NUL，lastEventId 保持 evt-4',
    '',
    'data:',
    '',
    ':!delay 1200',
    'id: evt-5',
    'data: 静默 1200ms 后的事件',
    '',
    ':!disconnect',
    'id: evt-6',
    'data: 断线前未以空行结束 —— 本事件应被丢弃',
    ':!expect-id evt-6',
    'event: resumed',
    'id: evt-7',
    'data: 重连成功，上一次有效 Last-Event-ID 为 evt-6（断线前已收到 id 行）',
    '',
    'id: evt-8',
    'data: 第一次收到，正常派发',
    '',
    'id: evt-8',
    'data: 重复的 evt-8，应被去重',
    '',
    'retry: 400',
    'id: evt-9',
    'data: 服务端把重连间隔更新为 400ms',
    '',
    ':!disconnect',
    'id: evt-10',
    'data: 未派发（无空行即断线），但 id 行已生效',
    ':!expect-id evt-10',
    'id: evt-11',
    'data: 第二次重连成功，退避已按 400ms 基准计算，续传携带 evt-10',
    '',
    'event: bye',
    'id: evt-12',
    'data: 全部场景演示完毕',
    '',
  ].join('\n');
}

export function stressScript(n = 10000) {
  const L = [];
  for (let i = 0; i < n; i++) {
    L.push(`id: s-${i}`);
    L.push(`event: tick`);
    L.push(`data: {"i":${i},"payload":"${'x'.repeat(20)}"}`);
    if (i === 1000) {
      L.push(': 中途注释行');
      L.push('weird-field: ignored');
    }
    if (i === 4000) L.push('retry: abc');
    if (i === 5000) {
      L.push('');
      L.push(':!disconnect');
    }
    if (i === 7000) {
      L.push(`id: s-${7000 - 1}`);
      L.push(`data: duplicate id should be deduped`);
    }
    L.push('');
  }
  return L.join('\n');
}
