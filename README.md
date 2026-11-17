# consumer-group-rebalance-coordinator

JavaScript 消费组重平衡协调库，处理 generation、协作撤销、会话和重复提交。

## 核心概念

- **会话与心跳**：成员通过 `join(memberId, topics)` 上报订阅并建立会话，之后靠
  `heartbeat(memberId)` 维持。超过 `sessionTimeoutMs` 未心跳的成员由 `tick()` 移除，
  其分区立即变为可分配（崩溃的 owner 无法确认撤销）。
- **Generation**：每次 assignment 发生变化，generation 递增。所有 `commit` 和
  `ackRevocation` 都携带 generation；与当前 generation 不符的一律以
  `STALE_GENERATION` 拒绝（fencing），防止旧成员在 rebalance 后提交位移。
- **Assignment diff**：每次 rebalance 都会向 `rebalanceLog` 追加一条
  `{ generation, reason, added, removed, pendingRevocation }` 记录，并触发
  `'rebalance'` 事件。

## Eager 与 Cooperative 的差异

**Eager（stop-the-world）**：一次 rebalance 完成全部切换。成员关系或订阅变化时，
所有分区立即按新 assignment 生效，旧 owner 直接失去分区。优点是只经过一代；
缺点是旧 owner 可能还在处理分区时新 owner 已经开始消费同一分区（重复消费窗口），
且未移动的分区也会被中断。

**Cooperative（增量、两阶段）**：

1. 第一阶段只下发**撤销**：需要迁移的分区进入旧 owner 的 `revoking` 集合，
   新 owner 暂时拿不到它们，未涉及的分区完全不受影响。
2. 旧 owner 执行完撤销回调后调用 `ackRevocation` 确认；协调器随后触发一次
   后续 rebalance，把已释放的分区交给新 owner。

这保证了**同一分区同一时间只有一个 owner**，但引入了新的 fencing 要求：
旧 owner 在撤销回调完成前，可能拿着新 generation 去提交待撤销分区的位移——
协调器通过「提交时校验分区仍在 `owned` 集合中（`revoking` 不算）」拒绝这类提交
（`ILLEGAL_ASSIGNMENT`）。若新 owner 在 ack 前退出，撤销会被取消，分区回到旧 owner。

## 协调器重启

generation 与已提交位移通过 `MemoryStore`（可替换为任意实现 `load/save` 的存储）
持久化。协调器重启后会话状态丢失，成员必须重新 join；持久化的 generation 保证
重启前的旧 generation 提交依然被拒绝，位移不丢失。

## API

```js
const { Coordinator, MemoryStore } = require('./src');

const coordinator = new Coordinator({
  groupId: 'g1',
  partitionsByTopic: { events: 6 },
  protocol: 'cooperative', // 或 'eager'
  sessionTimeoutMs: 10_000,
  store: new MemoryStore(),
});

coordinator.on('rebalance', (diff) => console.log(diff));

const view = coordinator.join('consumer-1', ['events']);
// view: { memberId, generation, assignment, revoking, state }
coordinator.heartbeat('consumer-1');
coordinator.ackRevocation('consumer-1', view.generation, view.revoking);
coordinator.commit('consumer-1', view.generation, [
  { topic: 'events', partition: 0, offset: 42 },
]);
coordinator.tick(); // 检查会话超时
```

## 运行

```sh
npm test    # node --test，覆盖成员崩溃、延迟撤销、重复 join、订阅变化、协调器重启
npm run demo # 对比 eager 与 cooperative 下每次 rebalance 的 assignment diff
```
