// 内置示例报文（@disconnect 为本地模拟指令，表示服务端断流）
const SAMPLES = {
  basic:
`event: message
data: hello sse
id: 1

event: notice
data: second event
id: 2
retry: 5000

`,
  multiline:
`event: poem
data: 第一行
data: 第二行
data: 第三行
id: 10

data: 只有 data 的默认 message 事件
id: 11

`,
  heartbeat:
`: this is a heartbeat comment

:ping

event: message
data: after heartbeat
id: 20

`,
  disconnect:
`event: message
data: before disconnect 1
id: 100

event: message
data: before disconnect 2
id: 101

@disconnect
event: message
data: resumed after reconnect
id: 102

`,
  dup:
`event: message
data: first copy
id: 200

event: message
data: duplicate id, should be dropped
id: 200

event: message
data: unique again
id: 201

`,
  invalid:
`foo: unknown-field
bar
retry: not-a-number
event: message
data: survives invalid fields
id: 300

retry: abc
event: message
data: retry fell back to default
id: 301

`,
};

function makeStress(n) {
  const parts = [];
  for (let i = 0; i < n; i++) {
    parts.push(`event: tick\ndata: payload-${i}\nid: ${i}\n`);
    if (i % 100 === 99) parts.push(': heartbeat\n');
    if (i % 2000 === 1999) parts.push('@disconnect\n');
  }
  return parts.join('\n');
}
