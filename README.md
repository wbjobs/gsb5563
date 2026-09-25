# SSE 解析器 · 状态机 · 续传模拟

纯本地前端工具：粘贴或加载 SSE 原始报文，按规范解析并用状态机还原事件流生命周期。不连接真实服务器。

## 运行

```bash
python3 -m http.server 8080
# 打开 http://localhost:8080
```

（必须通过 HTTP 访问，Web Worker 不支持 file:// 协议。）

## 功能

- **规范解析**（Web Worker + TypedArray 字节级解析，主线程零阻塞）
  - `event` / `data` / `id` / `retry` 字段；多行 `data` 以 `\n` 拼接
  - 兼容 `\n` / `\r\n` / `\r` 行尾；`id` 含 NUL 字符时按规范忽略
  - 注释行（`:` 开头）识别为心跳；未知字段、无冒号行忽略不崩
  - `retry` 非数字时降级为默认值；流末尾未分发缓冲按规范丢弃
- **状态机**：连接建立 → 事件接收 → 心跳 → 断线 → 按 retry 退避重连 → 携带 `Last-Event-ID` 续传
- **模拟指令**（独占一行）：`@disconnect` 模拟服务端断流
- **事件去重**：相同 `id` 的事件只接收一次
- **降级**：浏览器不支持 `EventSource` 时显示可读降级方案提示
- **IndexedDB**：事件日志批量持久化（单事务）
- **Canvas**：虚拟时间轴时序可视化（状态带 + 事件刻度）
- **压测**：一键生成 1 万条事件（含心跳与断流），解析在 Worker 中完成

## 文件结构

- `js/worker.js` — SSE 字节级解析器（Worker）
- `js/statemachine.js` — 事件流生命周期状态机（虚拟时间）
- `js/timeline.js` — Canvas 时序可视化
- `js/db.js` — IndexedDB 事件日志
- `js/samples.js` — 内置示例报文
- `js/app.js` — UI 组装
