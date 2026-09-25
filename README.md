# SSE Lab — SSE 解析 / 状态机 / 续传模拟器

纯前端工具（不连接真实服务器）：手动输入或本地加载一段 SSE 原始报文，按
[WHATWG HTML SSE 规范](https://html.spec.whatwg.org/multipage/server-sent-events.html)
解析 `event / data / id / retry` 字段，并用状态机还原
**连接建立 → 事件接收 → 心跳 → 断线 → 按 retry 退避重连 → 携带 Last-Event-ID 续传** 的完整生命周期。

## 运行

```bash
# ES Module + Web Worker 需要 http 环境，不能用 file:// 直接打开
python3 -m http.server 8080
# 打开 http://localhost:8080/
```

## 功能与技术栈

- **严格 SSE 解析**（`lib/parser.js`）：UTF-8 流式解码（`TextDecoder` + `TypedArray`）、
  按 `CRLF / LF` 切行、BOM 只剥一次、冒号后仅去一个空格、多行 `data` 用 `\n` 拼接、
  空行派发、注释忽略、未知字段忽略、`retry` 仅接受纯十进制、含 NUL 的 `id` 不更新 lastEventId、
  断流时丢弃未派发事件但保留 lastEventId。
- **Web Worker**（`worker/sse-worker.js`）：解析在后台线程进行，主线程通过
  Transferable `ArrayBuffer` 零拷贝喂入分块字节，避免 1 万条事件卡顿。
- **状态机**（`lib/fsm.js`）：`CONNECTING / OPEN / RETRY_WAIT / CLOSED / DONE`，
  断线后按服务端 `retry` 指数退避（±15% 抖动，30s 封顶）重连，请求携带 `Last-Event-ID`。
- **事件去重**：按 `id` 在内存 Set + IndexedDB 双重记录，重连后重复事件丢弃并打点。
- **IndexedDB**（`lib/db.js`）：完整事件日志、已见 ID 集合、最近一次 Last-Event-ID 持久化；
  支持刷新恢复、JSON 导出、清空。
- **Canvas 时序图**（`js/timeline-view.js`）：会话泳道 + 事件竖条 + 连接/心跳/重连/去重/异常标记，
  滚轮缩放、拖拽平移、悬停 tooltip、点击选中、自动跟随。
- **降级方案**：不支持 `EventSource` 时给出可读降级提示（fetch+ReadableStream / XHR 轮询）；
  `Worker` 不可用时解析自动降级到主线程并提示。

## 模拟指令（写在注释里，属于脚本语法的一部分）

| 指令 | 含义 |
| --- | --- |
| `:!delay 1200` | 服务端静默 1200ms（观察心跳/超时） |
| `:!disconnect` | 服务端中途断流（未以空行结束的事件应丢弃） |
| `:!expect-id <id>` | 校验本次（重）连接携带的 Last-Event-ID |

控制栏可调：播放速度、分片字节大小、**混沌切分**（1~N 随机切分，压测跨 chunk/跨多字节字符）、最大重连次数。

## 验收对照

- 字段解析 / 多行 data / 注释 / 非法字段 / 非法 retry / NUL id / 断流续传 —— `tests/parser.test.mjs`（45 项）
- 编译 → 模拟 → 退避重连 → Last-Event-ID 续传 → 去重 → 重连上限 —— `tests/simulator.test.mjs`（13 项）

```bash
node tests/parser.test.mjs
node tests/simulator.test.mjs
```

- 1 万条事件：点“生成 1 万条压测”后开始，Worker 后台解析（统计栏显示累计解析耗时），
  列表只渲染最近 5000 条、Canvas 按视窗裁剪，保证主线程不卡。
