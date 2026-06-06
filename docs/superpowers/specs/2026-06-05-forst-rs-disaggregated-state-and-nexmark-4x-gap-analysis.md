# forst-rs 分离式远程状态与 Nexmark 性能追齐差距分析

**日期：** 2026-06-05

**范围：**

- ForSt 代码库：`/Users/lijunqing/Code/stczwd/ForSt`
- Flink 代码库：`/Users/lijunqing/Code/stczwd/flink`

**目标验收：**

- 本地 forst-rs 后端：冲刺目标是 Nexmark 端到端吞吐达到 **4.x 加速**；保底目标是相比
  本地 RocksDB 至少 **2.x 加速**。
- 远程/分离式 forst-rs 后端：冲刺目标是相比本地 RocksDB 达到 **2.x 加速**；保底目标是相比
  本地 RocksDB 至少 **1.5.x 加速**。
- 冲刺目标未达成不阻断开发。只要达到保底目标，就继续推进下一阶段，同时记录未达冲刺目标的
  查询、场景、火焰图和根因。
- 短期第一目标是本地性能闭环。只有本地 forst-rs 达到准确性通过、吞吐稳定、至少 2.x 保底后，
  才启动远程 1.5.x 性能目标的工程闭环；远程设计可以提前准备，但不得抢占本地 P0/P1 的主线资源。
- 每完成一个关键功能或 PR，必须先完成准确性确认和性能确认，再进入下一批集成。准确性确认覆盖
  Nexmark 所需状态语义、checkpoint/restore、key-group 隔离和目标查询结果；性能确认覆盖
  RocksDB/ForSt/forst-rs 对比、目标查询多轮复测、吞吐低谷、RSS/堆外内存、检查点耗时和火焰图。
- 结果必须是稳定的端到端运行，不接受短时间冷启动峰值。q4、q5、q8、q9、q11、q12
  不能隐藏吞吐锯齿、检查点阻塞、恢复全量下载、内存超配等问题。

**当前范围：**

- 当前文档只讨论 Nexmark 性能达标。
- 仍然必须保证 Nexmark 所需正确性：状态语义、key-group 隔离、checkpoint/restore 一致性、
  远程对象可见性、缓存一致性和基准可复现性。
- 任何不直接服务本地保底 2.x、后续远程保底 1.5.x，或进一步冲击本地 4.x、远程 2.x 的内容，
  都不进入当前文档；短期排序始终是先本地、后远程。

**硬性边界：**

- 不修改 Flink Runtime 引擎。
- 不修改 Flink streaming 算子、调度、网络、mailbox、checkpoint coordinator、SQL runtime
  执行链路。
- 不要求上层算子改成新的批量状态调用协议。
- 可以修改的范围仅限：`flink-statebackend-forst-rs` 后端模块、ForSt-RS Java 状态实现、FFM
  linker、ForSt-RS Rust 引擎、ForSt/ForSt-RS 引擎侧远程文件和缓存实现、后端配置和指标。
- 所有“批量化”“向量化”“预取”“合并写”都必须隐藏在既有 StateBackend/State/Timer/Snapshot
  接口之下，对 Flink Runtime 和算子保持透明。

**硬性性能约束：**

- 不得破坏端到端向量化、批执行/列式批执行模型、Arrow 列式数据结构和零拷贝语义。
- 状态访问和序列化热路径不得退回 Java 字节数组形态；FFM 边界、ForSt-RS Java 状态实现、
  Rust 引擎读写接口必须以 arena、内存段、Arrow buffer、offset/length、列式 batch 为基本形态。
- 禁止在后端和引擎热路径执行标量状态访问、标量序列化、标量 FFM、标量压实、标量 iterator
  消费。Flink 既有接口如果表现为标量入口，ForSt-RS backend 必须立即进入写缓冲、读合并、
  范围预取、iterator chunk、批量 FFM 或 Rust 向量化执行。
- 坚持“性能优先、正确性随行”：正确性不能滞后到最后补救，而是每一次性能重构都要同步保留状态
  语义、key-group 隔离、checkpoint/restore 一致性和对象生命周期不变量。
- 允许为性能推翻已有实现。对已经证明有结构性缺陷的路径，不做缝补式修复，优先重构为批量化、
  向量化、低锁竞争、零拷贝路径。
- 充分复用 v3.8 中被证明为真实收益的设计，同时借鉴 RocksDB、ForSt、Apache Fluss 的状态后端
  思路；优先吸收增量检查点、低锁/无锁竞争、流式更新、写入合并、共享缓存、后台任务隔离等手段。
- 所有重构都必须落文档到 `docs/superpowers/specs/`，并在事后做端到端基准验证。未达预期时，
  必须记录问题场景、根因和回退理由，避免未来重复踩坑。
- 所有关键功能和 PR 都要带准入门禁：先跑准确性，再跑性能。没有准确性结果和性能 A/B 证据的
  补丁只能保留为实验分支，不能进入主控集成队列。
- 从第 1 天开始即按最多 10 个独立 subagent 工作流并行开发。并发不是为了增加噪声，而是为了
  提升吞吐、快速暴露跨层瓶颈，并在一周内尽可能完成 Nexmark 性能追齐。

## 核心结论

当前 forst-rs 的方向是正确的，但剩余工作远大于一次 Rust 引擎热点路径重构。为了在 Nexmark 上
达到本地保底 2.x、远程保底 1.5.x，并继续冲击本地 4.x、远程 2.x，forst-rs 必须形成四条后端
可控主线：

1. Flink 槽位级资源所有权和 StateBackend 能力契约。
2. 远程优先的 SST 生命周期和检查点/恢复语义。
3. Rust LSM 引擎的调度、内存、缓存、压实和远程文件模型。
4. ForSt-RS backend 内部状态访问协议，在既有 StateBackend/State/Timer 接口之下完成批量化
   和向量化。

`docs/superpowers/specs/2026-06-05-slot-shared-resource-model-design.md` 中的槽位共享资源模型是
q4 衰退的必要 P0 修复，但它不是本地 4.x / 2.x 和远程 2.x / 1.5.x 的最终解。它解决的是一个明确的结构性
故障：后台刷新/压实 CPU 随算子子任务数量放大。更大的目标还需要继续消除
标量 Java/Rust FFI、重复序列化与复制、LSM 压实物化、timer/MapState 标量迭代器、远程恢复和
检查点复制等成本。

在不修改 Flink Runtime、且只追求 Nexmark 性能的约束下，当前工作应按 **10-14 周、3-5 名
工程师** 的基准冲刺来组织。它必须依赖后端内部状态实现、FFM 和 Rust 引擎把标量上层调用“吞掉”，
而不是依赖算子/运行时改造。

阶段顺序必须收紧：第一阶段只以本地准确性和本地性能为主，远程能力只做接口、命名空间和验证脚本
准备；第二阶段在本地保底达成后，再把远程 1.5.x 作为工程闭环目标。这样即使本地冲刺 4.x 暂未
达成，也不会阻断后续远程目标，但远程目标不能早于本地保底确认。

## ForSt 是怎么做到的

ForSt 不是简单的“换名 RocksDB”。它是 RocksDB 家族引擎在 Flink 中的深度集成版本，核心优势有两点：

- 继承成熟的 RocksDB/ForSt 原生 LSM：块缓存、WriteBufferManager、后台 Env 线程池、
  过滤器、表格式、活跃文件检查点捕获等。
- 加入 Flink 感知的远程文件系统和状态句柄复用，使远程状态成为 DB/检查点生命周期的一部分，
  而不是“本地 DB 之后再上传”的附属动作。

源码锚点：

- `flink-statebackend-forst/ForStSharedResourcesFactory.java` 通过 Flink `MemoryManager` 或
  `SharedResources` 分配槽位级/TaskManager 级共享资源。
- `flink-statebackend-forst/ForStResourceContainer.java` 将共享 WriteBufferManager 写入
  DBOptions，并将共享块缓存写入 ColumnFamilyOptions。
- `ForStResourceContainer.getDbOptions()` 在配置远程 ForSt 路径时安装 `FlinkEnv` 和
  `StringifiedForStFileSystem`。
- `ForStKeyedStateBackendBuilder` 对 `IncrementalRemoteKeyedStateHandle` 选择
  `ForStIncrementalRestoreOperation`，并创建 `ForStIncrementalSnapshotStrategy`。
- `DataTransferStrategyBuilder` 在 DB 远程路径和检查点共享状态文件系统兼容、且声明模式
  允许时，可以选择 `ReusableDataTransferStrategy`，而不是复制策略。
- ForSt 的检查点和恢复路径与远程文件复用策略是同一个体系，而不是分散的后处理逻辑。

这解释了 ForSt 相比普通本地 RocksDB 的架构优势：SST 文件、检查点共享状态、恢复策略、
本地缓存和 Flink 资源生命周期是一个整体系统。

## 当前 forst-rs 已经具备什么

forst-rs 已经有一些关键能力，但它们还没有组合成面向 Nexmark 远程保底 1.5.x / 冲刺 2.x 的远程状态后端。

### 已有优势

- Rust 引擎通过 FFM 暴露本地和远程 open 路径，包括 `dbOpenRemoteWithOptions`。
- `ForStRsSnapshotStrategy` 已经输出标准 `IncrementalRemoteKeyedStateHandle`。
- `ForStRsRestoreOperation` 已经接受标准 `IncrementalRemoteKeyedStateHandle`。
- `ForStRsSstUploader` 已有有界并发上传。
- Rust 侧通过 `WriteBufferManager::new_global` 具备全局 WBM 预算。
- Rust 侧在 `column_family.rs` 中具备常驻影子状态全局预算。
- 当前工作区已经有有界后台工作线程池：`bg_pool.rs`，以及 `db.rs` 中的 `bg_flush_pool` 和
  `bg_compact_pool`，与 q4 的槽位共享资源设计方向一致。
- Flink forst-rs 后端中，timer 和 MapState 已有查询相关性能补丁：更大的 timer 刷新阈值、
  范围迭代器补充、恢复游标、自适应 MapState 缓存绕过。

### 主要架构差距

1. `ForStRsSharedResourcesFactory` 仍然只是一个绑定到单个 `FrsDb` 的资源视图。它自己的注释也
   说明按 TaskSlot 共享尚未完成，当前每个后端都有自己的引擎和共享资源视图。这不同于
   ForSt 的 `OpaqueMemoryResource` 生命周期。

2. 远程 open 通过给配置的 storage URI 加每个后端实例独有的标记来避免 SST 文件名冲突。这个
   修复对安全性是必要的，但它不是一个共享的分离式状态命名空间，也没有对象所有权、引用计数、
   检查点级复用语义。

3. 恢复仍然偏向“下载状态句柄后从增量状态打开”。`ForStRsRestoreOperation` 已经接受
   `IncrementalRemoteKeyedStateHandle`，但还没有形成围绕 Nexmark 远程保底 1.5.x / 冲刺 2.x 的可复用远程文件
   传输策略。

4. 快照虽然输出标准 Flink 增量状态句柄，但远程 SST 生命周期还不是 ForSt 那种：在合适
   文件系统/声明模式条件下，DB 远程文件可以被检查点直接复用。

5. Rust 块缓存仍然由每个 `DbImpl` 通过 `ShardedClockCache::with_capacity(...)` 打开。
   共享缓存需要在缓存键中加入 DB/table salt，并解决分片锁竞争。之前的共享缓存尝试因为跨实例
   分片锁竞争被回退过。

6. forst-rs 目前在累积 timer、MapState 的查询专用修复。这些修复有价值，但还不是一个稳定的
   后端内部状态访问协议，无法单独支撑 Nexmark 全套 4.x 端到端加速。

7. Rust LSM 引擎在多个路径上仍有高于 RocksDB/ForSt 的常数成本：细粒度复制、memtable 键/索引
   分配、压实物化、范围扫描解码、缓存查找/更新。

## RocksDB、ForSt、forst-rs 对比

| 维度 | RocksDB 后端 | ForSt 后端 | 当前 forst-rs | 保底/冲刺所需形态 |
|---|---|---|---|---|
| 槽位内存 | Flink 共享 WBM/缓存 | Flink 共享 WBM/缓存 | 多数仍是每个后端一份资源视图，Rust 全局 WBM 已开始补齐 | Flink 拥有的槽位级资源对象，被所有 forst-rs DB 共享 |
| 后台 CPU | RocksDB Env pool | RocksDB/ForSt Env pool | Rust 全局后台池正在补齐 | 槽位级有界后台池，加压实/限速反馈 |
| 远程 DB | 无远程优先 DB | 通过 FlinkEnv 使用远程 ForSt 路径 | 每个后端实例一个 OpenDAL 远程路径 | 远程优先命名空间，具备 SST 所有权和租约 |
| 检查点 | 增量 SST 状态句柄 | 增量 SST 状态句柄加远程复用策略 | 已输出标准增量状态句柄 | 条件允许时直接注册远程 SST，避免重复复制 |
| 恢复 | 下载/重连本地 SST | 按文件系统/声明模式选择复制或复用 | 从增量状态下载并打开 | no-claim 可复用远程恢复，支持 rescale |
| 状态访问 | JNI 标量访问，成熟引擎 | JNI 标量访问，成熟引擎 | FFM 标量访问加部分向量化 | 既有 StateBackend 接口下的后端内部批量/向量化 |
| timer | 堆或 Rocks/ForSt PQ | 堆或 ForSt PQ | timer queue 正在修复 | 引擎级 timer 批量和范围补充 |
| MapState | 成熟迭代器/缓存行为 | 成熟迭代器/缓存行为 | 重连接中缓存抖动，已有绕过补丁 | 可预测缓存策略，加向量化 prefix/range API |

## 为什么槽位共享资源只是 P0

q4 设计文档证明了一个根因：后台刷新/压实并发随“算子 x 并行度”放大，形成
12 个刷新线程和 12 个压实线程，进而抢占前台区间连接的 CPU。新的 Rust
`WorkerPool`、`bg_flush_pool`、`bg_compact_pool` 正是针对这个根因。

这个修复应当能拉平吞吐锯齿，但它不会自动带来 4.x 加速。拉平后的 q4 仍然会有这些成本：

- V1-sync 算子仍然按记录调用状态，并跨越 FFM 边界。
- key/value 序列化仍然经常物化 Java 字节数组或复制内存段。
- 压实仍然消耗大量 CPU，因为部分路径会解码/重编码，甚至先物化再写出。
- 范围扫描和前缀迭代器需要将批量行一直送到 Flink 算子层，而不是只在引擎内批量。
- MapState 和 timer 仍然是分散的局部修复，还没有收敛成统一的批量状态访问基底。
- 远程路径仍然会付出对象可见性、上传屏障、恢复下载、本地缓存填充成本，除非重做 SST
  生命周期。

所以槽位共享资源模型应该被视为第一个正确性/性能验收关口，而不是最终架构。

## 本地性能目标架构

要达到本地保底 2.x 并继续冲击 4.x，forst-rs 不能只是一个更快的标量 RocksDB 克隆。但在不修改 Flink
Runtime 的边界下，它也不能要求区间连接、窗口、聚合等算子主动发出新的批量状态调用。正确方向是：
保持 Flink 上层看到的 StateBackend/State/Timer 接口不变，在 ForSt-RS backend 和 Rust 引擎内部
把标量调用尽可能合并、缓存、预取、向量化。

### 1. ForSt-RS backend 内部批量化协议

需要设计：

- 不改变 Flink Runtime 和算子调用形态，只在 ForSt-RS backend 内部新增批量点查、批量写入、
  删除、前缀扫描、范围扫描 FFM API。
- V1-sync 的上层调用即使表现为标量入口，backend 内部也要通过写缓冲、读缓存、范围预取、
  iterator chunk、timer 批量 drain/refill 来摊薄 FFM 和序列化成本。
- 使用直接 arena/内存段作为输入输出，避免 Java 字节数组形态进入状态访问路径。
- 返回可直接消费的解码后行批次或稳定字节切片，尽量避免 ForSt-RS 状态实现立即复制。
- ForStRsMapState、ForStRsKeyGroupedInternalPriorityQueue、ForStRsKeyedStateBackend 的写缓冲、
  prefix/range iterator、snapshot pre-hook 是第一批落点。
- 让 V1-sync 和 V2-async 在后端内部尽可能共用同一套 FFM batch primitive，但不要求上层算子知道它。

这是本地保底 2.x 和冲刺 4.x 最关键的一条线。没有它，q4 即使稳定下来，前台仍会消耗太多 CPU 在 Java 序列化、
FFM 调用、标量状态查找、迭代器打开和标量合并逻辑上。由于不能修改 Flink Runtime，这些成本
只能由后端内部缓存、合并写、预取、批量 FFM 和 Rust 引擎向量化来吸收。

### 2. Rust LSM 读写路径

需要设计：

- key/value 使用 arena/offset 结构，避免 arena、BTreeMap、HashMap、常驻影子状态、缓存元数据
  之间重复持有 key。
- 实现向量化 memtable/SST 查找，一次遍历服务一个批次。
- 压实输入输出流式化，避免构造大 `Vec<CompactionEntry>`，可以转发 key/value 字节的路径不做
  解码/重编码。
- 对重复扫描的工作负载增加解码块缓存或前缀块缓存。
- 增加各层级/各 CF 压实债务指标、压实限速和前台感知调度。
- 常驻影子状态应该是可选策略，而不是默认无限接近常驻状态副本。

干净的设计目标是：一个热点 q4 查找通常应该表现为一次批量 FFM 调用、一次向量化
memtable/SST 探测，并且没有可避免的 Java 字节数组分配。

### 3. 槽位共享本地资源

需要设计：

- Flink 创建一个槽位级 forst-rs 资源对象，类似 ForSt 的
  `OpaqueMemoryResource<ForStSharedResources>`。
- 这个资源对象拥有 WBM、块缓存、本地文件缓存预算、解码块缓存预算、后台调度器、
  限速器和指标。
- Rust 引擎实例注册到这个资源对象，而不是各自创建独立预算。
- 共享块缓存的缓存键中必须包含 DB/table salt，并具备足够分片，避免锁竞争。
- 内存压力要反馈到 Flink 指标和反压体系，而不只是依赖 Rust 环境变量。

当前全局 WBM/常驻计数器和 Rust 后台池是好的起点，但最终模型必须由 Flink 拥有并保证生命周期安全。

## 远程性能目标架构

本节是第二阶段目标架构。短期只允许做远程契约、命名空间、脚本和风险准备；真正进入远程性能闭环，
必须等本地准确性通过且本地 forst-rs 至少达到 2.x 保底。

远程保底目标是相比本地 RocksDB 至少 1.5.x，冲刺目标是 2.x。它比“把 S3 做快”要求高得多。系统必须让远程状态对热前台路径近似
不可见，同时保留远程持久化和快速检查点/恢复。

### 1. 远程优先 SST 命名空间

需要设计：

- 每个作业/算子/后端拥有稳定的远程命名空间，包含 manifest、SST 对象、所有权、租约和清理规则。
- SST 文件身份必须由 DB 实例、generation、file number 全局唯一决定，而不是只靠临时 URI 后缀
  避免冲突。
- 远程对象需要明确状态：写入中、可见、检查点持有、共享、废弃。
- 上传屏障与 manifest publication 绑定，避免 reader 观察到半可见 SST。
- manifest 更新和检查点句柄创建使用同一套对象身份模型。

这是分离式状态设计缺失的中心。它把对象存储从“上传文件的位置”变成状态文件的事实源。

### 2. 零复制检查点和可复用恢复

需要设计：

- 如果 DB 远程路径和检查点共享状态文件系统兼容，检查点直接注册现有远程 SST 状态句柄，
  而不是复制/上传重复字节。
- 如果 recovery claim 模式允许复用，恢复直接映射/复用远程 SST 状态句柄，而不是下载所有文件到新的
  本地 DB。
- 对 Nexmark 使用的 key-group 范围和检查点路径，恢复只做必要 SST 物化，避免全量本地展开。
- SharedStateRegistry 成为检查点持有 SST 生命周期的权威，避免重复上传和重复下载。

ForSt 通过 `FlinkEnv`、`ForStFlinkFileSystem`、`DataTransferStrategyBuilder`、
`ReusableDataTransferStrategy`、标准 `IncrementalRemoteKeyedStateHandle` 已经具备这个概念形态。
forst-rs 需要 Rust/OpenDAL 等价实现，并只围绕 Nexmark 远程保底 1.5.x / 冲刺 2.x 路径补齐必要集成。

### 3. 本地缓存和预取

需要设计：

- 一个槽位级/TaskManager 级本地文件缓存，具备 O(1) 或分片 LRU 行为，并有显式字节预算。
- 并发读取同一个 SST/block 时进行请求合并。
- 为区间连接、窗口扫描、timer drain 做范围感知预取。
- 为 CPU 受限的重复扫描做解码块缓存。
- negative/visibility 缓存只能在对象存储一致性规则允许时启用。
- 缓存准入策略基于复用价值，而不是“所有远程读取都缓存”。

远程 1.5.x/2.x 不会单靠 S3 带宽获得。q4/q9 这类工作负载经常受随机探测、重复前缀扫描、
迭代器解码和前台 CPU 限制。缓存必须同时减少 I/O 和 CPU。

## 一周 LLM PMC Subagent 攻坚机制

当前推进方式不能再是单线程调研、单线程实现、单线程验证。为了在一周内最大化追齐速度，设计上采用
“一个主控 + 第 1 天起最多 10 个 Flink PMC 技术专家 subagent”的并行开发机制。每个 subagent
只负责一个互不重叠的技术战线，输出可落地文档、代码候选方案、基准证据和失败根因。主控只负责
统一约束、冲突裁决、最终集成和端到端验证。

### 第 1 天并行启动的 10 个专家槽位

第 1 天即按 A-J 十个专家槽位铺开。若执行资源不足，A-E 是必启槽位，F-J 也不再等待后续扩展，
而是根据 q11/q12、MapState、compaction、社区对照、集成审查的即时需要在第 1 天内拉起。

| subagent | PMC 专家视角 | 独立战线 | 一周内必须产出 |
|---|---|---|---|
| A. StateBackend/FFM 向量化专家 | Flink StateBackend 和 FFM 桥接 | ForSt-RS Java 后端、linker、arena/内存段接口、批量 FFM 基础接口 | 无 Java 字节数组热路径方案；MapState/timer/写缓冲的批量 FFM 设计；可验证补丁或最小原型 |
| B. Rust LSM 引擎专家 | RocksDB/ForSt LSM 与 Rust 数据结构 | memtable、SST lookup、compaction、arena/offset index、零拷贝读写 | 向量化 memtable/SST 查找方案；压实流式化方案；去除细粒度复制和行物化的改造清单 |
| C. 远程 SST/Checkpoint 专家 | ForSt 远程状态和 checkpoint 文件复用 | OpenDAL、远程 SST 命名空间、manifest、零复制 checkpoint、no-claim 最小恢复 | 本地达标前只产出远程最小闭环设计和脚本；本地确认后再进入远程 1.5.x 工程闭环 |
| D. 槽位资源/缓存专家 | Flink managed memory、RocksDB/ForSt shared resource | WBM、block cache、file cache、decoded cache、后台 flush/compaction pool | q4 锯齿消除验证方案；槽位级共享资源和缓存分片方案；锁竞争风险与指标 |
| E. Nexmark 基准/剖析专家 | Flink 基准、火焰图、JFR、端到端验收 | q0-q22 基准协议、q4/q5/q8/q9/q11/q12 热点剖析、对比证据 | 每日基准榜单；每个关键功能/PR 的准确性和性能双确认；本地优先差距台账 |
| F. Timer 专家 | Flink timer service 和 priority queue | ForStRsKeyGroupedInternalPriorityQueue、timer batch drain/refill、tombstone cleanup | q11/q12 timer 热路径改造；timer 批量化补丁或失败根因 |
| G. MapState 专家 | Flink MapState 和 prefix/range iterator | ForStRsMapState、cache bypass、prefix/range scan、decoded cache | q4/q5/q8/q9 MapState/cache 热路径改造；低命中绕过和向量化 iterator 方案 |
| H. Compaction 专家 | LSM compaction scheduler 和写放大 | L0/L1 触发、compaction debt、流式 compaction、后台限速 | q4 锯齿消除后的压实 CPU 降低方案；流式压实补丁或失败根因 |
| I. Fluss/RocksDB/ForSt 对照专家 | Apache Fluss、RocksDB、ForSt 设计对标 | 社区状态后端机制迁移和性能对照 | 可迁移机制清单；对 forst-rs 的具体实现建议；不直接改代码 |
| J. 集成审查专家 | Flink PMC 代码审查和正确性约束 | 检查 9 个 subagent 产物是否违反硬性边界、向量化、零拷贝、Nexmark 目标 | 每批补丁合并前的边界审查；冲突/退化/回退建议 |

### 一周节奏

| 时间 | 主控动作 | subagent 并行动作 | 验收物 |
|---|---|---|---|
| 第 0 天 | 冻结基准命令、配置、硬性约束和文件边界 | E 建立 RocksDB/ForSt/forst-rs 本地/远程基线 | 基线表、运行脚本、已知差距 |
| 第 1 天 | 分发最多 10 个战线任务 | A-J 独立读代码、列出热路径和第一批补丁目标 | 10 份战线设计记录或明确跳过理由 |
| 第 2 天 | 选择最高收益补丁队列 | A/B/D/F/G/H 做本地保底 2.x / 冲刺 4.x 相关原型；C 只做远程最小闭环设计；E 做对比验证模板；I/J 做对照与审查 | 第一批补丁或最小原型；每个补丁的准确性和性能验收命令 |
| 第 3 天 | 集成不冲突改动 | E 跑 q4/q5/q8/q9/q11/q12；A/B/D 根据火焰图迭代；J 审查 PR 级双验证是否齐全 | q4 锯齿是否消失；本地差距更新；未过门禁补丁清单 |
| 第 4 天 | 执行本地达标门禁 | 若本地已过准确性和 2.x 保底，C/D/E 才启动远程 checkpoint/cache/restore；否则 A/B/D/F/G/H 继续压本地 CPU | 本地门禁结论；满足门禁后才生成远程 1.5.x 差距更新 |
| 第 5 天 | 重排 10 个并发槽位 | 对剩余主导瓶颈重新分配 subagent，必要时让两个专家顺序接力但不并改同一文件 | 第二批补丁与风险清单 |
| 第 6 天 | 冻结候选组合 | E 跑全量 q0-q22 和目标查询多轮复测；若远程尚未启动，只冻结本地候选组合 | 本地成绩单；远程是否准入的明确结论 |
| 第 7 天 | 复盘是否达标 | 所有 subagent 提交失败根因、可保留收益、回退项 | 最终一周追齐报告 |

### 协作约束

- 每个 subagent 必须像 Flink PMC 技术专家一样输出：结论、源码锚点、性能假设、实现边界、验证命令、
  失败回退条件。
- 每个 subagent 的输出必须落到 `docs/superpowers/specs/`，不能只停留在聊天记录。
- subagent 之间不共享隐式上下文。主控给每个 subagent 明确输入、文件边界、禁止事项和验收物。
- subagent 不直接合并彼此改动；主控按基准收益和边界约束做集成。
- 若任一方案引入 Java 字节数组热路径、标量热路径、破坏 Arrow/列式/零拷贝语义，直接判定失败。
- 未达预期必须记录失败场景和根因，不允许只写“效果不好”。

## Nexmark 性能差距清单

### q4：区间连接

当前已证明的问题：

- 无界后台压实 CPU 导致吞吐锯齿崩塌。槽位共享后台池是 P0 修复。

剩余差距：

- 吞吐锯齿拉平后，标量状态访问和标量 FFM/序列化仍会主导前台成本。
- 连接状态访问需要批量点查/范围查找和向量化 merge-chain 处理。
- 压实必须在 CPU 和分配器压力上都不再抢占前台连接。

目标：

- 本地吞吐稳定，无深度吞吐低谷。
- 连接状态调用批量化，使前台不再像“更快引擎上的 RocksDB 标量访问”。

### q5/q8/q9：重连接、去重、prefix/range 压力

当前问题：

- 前缀扫描、常驻影子状态策略、块缓存大小和压实相互影响。
- 当工作负载以随机探测为主时，远程路径不能充分受益于原始带宽。

剩余差距：

- 需要向量化 prefix/range iterator，把批量结果直接送入 Java，避免行对象抖动。
- 需要解码块/前缀缓存服务重复扫描。
- 压实输出/读取路径需要避免不必要的字节复制。

### q11/q12：timer 和窗口密集路径

当前问题：

- timer drain/refill 过去频繁重新打开前缀迭代器，表现出接近 O(N^2) 的行为。
- 当前工作区补丁已提高刷新阈值、增加范围迭代器补充，并使用恢复游标。

剩余差距：

- timer 必须成为具备稳定内存和检查点行为的批量引擎服务，而不是孤立的 Java-side
  priority queue 修补。
- timer delete/tombstone 需要压实友好编码和批量清理。

### MapState-heavy 查询

当前问题：

- MapState 缓存在低命中工作负载中会抖动，增加 CPU/内存而不是减少成本。
- 当前工作区已经在缓存已满且窗口命中率低时自适应绕过。

剩余差距：

- 缓存策略应与引擎级 range/prefix access 和槽位缓存指标集成。
- MapState 迭代器需要向量化 key/value slice，而不是标量前缀遍历。

## 短期工作量拆解

### P0：稳定当前本地后端

目标：移除已知崩塌模式，并建立可信基准基线。

工作：

- 完成并验证槽位共享后台池。
- 将槽位/全局 WBM、常驻预算、本地文件缓存指标接入 Flink 可见指标。
- 增加带 salted key 和低锁竞争分片的安全共享块缓存，或者在共享缓存证明前严格收紧
  每个 DB 的缓存。
- 使用 q0-q22 和定向微基准验证 timer、MapState 补丁。
- 产出干净的本地 RocksDB、ForSt、forst-rs 本地、forst-rs 远程基准线。

工作量：**2-4 周，1-2 名工程师**，前提是当前未提交补丁能通过验证。

退出标准：

- q4 没有深度吞吐低谷。
- RSS 和堆外内存被槽位级预算约束。
- q0-q22 正确性通过。
- 每个进入集成队列的关键功能/PR 都有准确性结果、性能 A/B 结果、火焰图或等价剖析证据。

### P1：本地保底 2.x / 冲刺 4.x 架构原型

目标：在不修改 Flink Runtime 的前提下，让 forst-rs 成为后端内部向量化状态引擎，而不是只有
更低原生开销的标量引擎。

工作：

- 实现通用批量 FFM 状态协议，但只作为 ForSt-RS backend 内部 primitive 暴露。
- 改造 ForStRsMapState、ForStRsKeyGroupedInternalPriorityQueue、ForStRsKeyedStateBackend 写缓冲、
  prefix/range iterator、lookup/write path，使它们在既有 State/Timer 接口下内部使用批量 API。
- 移除 get/put/iterator 路径上的主要字节数组分配。
- 实现 arena/offset memtable index 和向量化查找。
- 流式化压实，消除压实热路径中的行物化。
- 为重复扫描增加解码块/前缀缓存。
- 增加后端内部读预取、请求合并、低命中缓存绕过和按 key-group/range 的自适应批量大小。

工作量：**8-12 周，3 名工程师**。相比允许修改 Runtime 的路线更难，因为无法让上层算子天然发出
批量状态请求。

退出标准：

- 本地 forst-rs 在约定 Nexmark 场景配置上相比本地 RocksDB 至少达到 2.x；4.x 作为冲刺目标，不作为中断条件。
- q4、q5、q8、q9、q11、q12 不因查询专用修补出现互相回归。
- 代码改动边界不越过 `flink-statebackend-forst-rs`、FFM linker 和 Rust/ForSt-RS 引擎侧。
- 本地准确性和性能门禁通过后，主控才能把远程 1.5.x 从设计准备切换为工程执行目标。

### P2：Nexmark 远程/分离式保底 1.5.x / 冲刺 2.x 架构

启动条件：P1 本地 forst-rs 至少达到 2.x 保底，且 q0-q22/目标查询准确性、checkpoint/restore
一致性、RSS/堆外内存、吞吐稳定性和 PR 级性能证据均已确认。

目标：围绕 Nexmark，使远程状态路径相比本地 RocksDB 至少达到 1.5.x 端到端加速，并继续冲刺 2.x。只保留会影响
基准性能和正确性的最小闭环。

工作：

- 设计并实现基准所需的远程 SST 命名空间、manifest 发布、对象状态、租约和清理。
- 实现远程 SST 的零复制检查点注册，避免基准期间重复 DB 复制。
- 实现同 key-group 范围下的可复用/no-claim 恢复，只保留基准直接需要的最小恢复能力。
- 让 OpenDAL/本地缓存行为符合 Nexmark checkpoint-on 场景的一致性要求。
- 增加槽位级/TaskManager 级文件缓存、解码块缓存、预取和请求合并。

工作量：**8-12 周，3-4 名工程师**。远程方案设计、脚本和接口契约可以与 P1 并行准备；远程工程
闭环必须等待 P1 本地门禁通过。

退出标准：

- 远程 forst-rs 在约定 Nexmark 场景配置上相比本地 RocksDB 至少达到 1.5.x；2.x 作为冲刺目标，不作为中断条件。
- 相同 key-group 范围的 no-claim 场景下，恢复不需要完整 SST 下载。
- 检查点耗时和上传字节量由新增/变化 SST 约束，而不是重复 DB 复制。
- 每个远程关键功能/PR 同样必须先通过准确性确认和性能确认，再进入下一批集成。

## 关键风险

1. **禁止修改 Flink Runtime 后，本地冲刺 4.x 是最高风险项。** 后端可以拉平 q4 并改善常数成本，
   但无法让区间连接、窗口、timer 算子主动发出新的批量状态操作。所有摊薄都必须发生在
   ForSt-RS backend、FFM 和 Rust 引擎内部。如果 P0/P1 后前台标量调用成本仍然主导，先以本地
   2.x 保底继续推进，并记录 4.x 差距。

2. **远程 1.5.x/2.x 不是 S3 带宽问题。** 路径必须避免前台随机远程读取、避免恢复下载、避免重复
   检查点复制。更快对象存储不能单独解决探测密集工作负载。远程工程闭环必须等本地 2.x 保底
   和准确性门禁确认后再启动；远程达到 1.5.x 即可继续推进，2.x 差距记录为后续冲刺项。

3. **共享块缓存可能变成锁竞争回归。** 必须加盐、分片、度量。朴素全局缓存
   可能降低 RSS，却伤害 q4。

4. **查询专用修复会掩盖框架缺失。** timer 和 MapState 补丁有价值，但性能设计应收敛到统一的
   批量/范围状态 API。

5. **任何需要 Flink Runtime 配合的优化都必须被视为越界。** 例如修改 interval join 算子批量访问
   状态、修改 mailbox 调度、修改 checkpoint coordinator、修改 SQL runtime 代码生成，都不属于本
   文档允许的实现范围。

## 推荐下一步

建议按以下顺序推进：

1. 冻结基准协议，记录 RocksDB、ForSt、forst-rs 本地、forst-rs 远程基准线。
2. 完成 P0 槽位共享资源模型，并验证 q4 吞吐锯齿消除。
3. 启动 P1 后端内部批量状态访问协议，以 ForStRsMapState、timer queue、写缓冲和 range iterator
   作为第一批牵引场景。
4. 每完成一个关键功能或 PR，先完成准确性确认和性能确认，再进入下一批集成。
5. 仅在本地准确性通过且本地 forst-rs 达到 2.x 保底后，启动 P2 远程 1.5.x 工程闭环；此前只并行
   设计远程 SST 命名空间和零复制检查点/恢复契约。
6. 若达到保底目标但未达到冲刺目标，不中断开发；记录差距、失败场景和根因，并继续围绕 Nexmark
   性能闭环迭代。

最终判断：短期只看 Nexmark。要在不修改 Flink Runtime 的前提下达到本地保底 2.x、远程保底 1.5.x，
并继续冲刺本地 4.x、远程 2.x，forst-rs 必须变成一个后端内部向量化、槽位管理、远程优先的状态系统。
只把它当成 Rust 版引擎重写，达不到这个目标；反过来，任何依赖 Runtime 或算子改造的方案也不符合本文边界。
短期执行上先证明本地，再进入远程；每个功能和 PR 都必须用准确性与性能证据说话。
