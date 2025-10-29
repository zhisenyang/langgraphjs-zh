# @langchain/langgraph-checkpoint

这个库为 [LangGraph.js](https://github.com/langchain-ai/langgraphjs) 检查点保存器定义了基础接口。检查点保存器为 LangGraph 提供持久化层。它们允许您与图的状态进行交互和管理。当您使用带有检查点保存器的图时，检查点保存器会在每个超级步骤保存图状态的_检查点_，从而启用多种强大功能，如人机交互、交互间的"记忆"等。

## 核心概念

### 检查点

检查点是图在给定时间点的状态快照。检查点元组指的是包含检查点以及相关配置、元数据和待处理写入的对象。

### 线程

线程使多个不同运行的检查点成为可能，这对于多租户聊天应用程序和其他需要维护独立状态的场景至关重要。线程是分配给检查点保存器保存的一系列检查点的唯一ID。使用检查点保存器时，运行图时必须指定 `thread_id`，可选择性地指定 `checkpoint_id`。

- `thread_id` 就是线程的ID。这是必需的
- `checkpoint_id` 可以可选地传递。此标识符指向线程内的特定检查点。这可以用于从线程中途的某个点开始运行图。

在调用图时，您必须将这些作为配置的可配置部分传递，例如：

```ts
{ configurable: { thread_id: "1" } }  // 有效配置
{ configurable: { thread_id: "1", checkpoint_id: "0c62ca34-ac19-445d-bbb0-5b4984975b2a" } }  // 也是有效配置
```

### 序列化/反序列化

`@langchain/langgraph-checkpoint` 还定义了序列化/反序列化（serde）协议，并提供了处理多种类型的默认实现。

### 待处理写入

当图节点在给定超级步骤的执行过程中失败时，LangGraph 会存储该超级步骤中成功完成的任何其他节点的待处理检查点写入，这样当我们从该超级步骤恢复图执行时，就不会重新运行成功的节点。

## 接口

每个检查点保存器都应符合 `BaseCheckpointSaver` 接口，并且必须实现以下方法：

- `.put` - 存储检查点及其配置和元数据。
- `.putWrites` - 存储链接到检查点的中间写入（即待处理写入）。
- `.getTuple` - 使用给定配置（`thread_id` 和 `thread_ts`）获取检查点元组。
- `.list` - 列出匹配给定配置和过滤条件的检查点。

## 使用方法

```ts
import { MemorySaver } from "@langchain/langgraph-checkpoint";

const writeConfig = {
  configurable: {
    thread_id: "1",
    checkpoint_ns: ""
  }
};
const readConfig = {
  configurable: {
    thread_id: "1"
  }
};

const checkpointer = new MemorySaver();
const checkpoint = {
  v: 1,
  ts: "2024-07-31T20:14:19.804150+00:00",
  id: "1ef4f797-8335-6428-8001-8a1503f9b875",
  channel_values: {
    my_key: "meow",
    node: "node"
  },
  channel_versions: {
    __start__: 2,
    my_key: 3,
    "start:node": 3,
    node: 3
  },
  versions_seen: {
    __input__: {},
    __start__: {
      __start__: 1
    },
    node: {
      "start:node": 2
    }
  },
  pending_sends: [],
}

// 存储检查点
await checkpointer.put(writeConfig, checkpoint, {}, {})

// 加载检查点
await checkpointer.get(readConfig)

// 列出检查点
for await (const checkpoint of checkpointer.list(readConfig)) {
  console.log(checkpoint);
}
```