/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;

/**
 * A change event source that emits events for taking a consistent snapshot of the captured tables, which may include
 * schema and data information.
 *
 * @author Gunnar Morling
 */
// SnapshotChangeEventSource 的主要作用是 执行全量数据读取并生成初始事件流。
// 在 CDC（数据变更捕获）过程中，通常不能直接从日志（如 Binlog）开始，因为日志只包含“变更”，不包含“历史存量数据”。该接口的实现类（如 RelationalSnapshotChangeEventSource）负责：
// 确定快照策略：根据配置决定是否需要快照（例如是全量快照、仅架构快照还是追加快照）。
// 获取一致性视图：通过锁表或事务隔离级别，确保读取到的所有表数据在时间点上是一致的。
// 读取架构与数据：扫描数据库表结构并导出所有现有记录，将其转换为 Debezium 事件发送到 Kafka。
// 标记位点：在快照完成时，记录下当前数据库日志的对应位置，以便后续增量阶段（Streaming）接管。
public interface SnapshotChangeEventSource<P extends Partition, O extends OffsetContext> extends ChangeEventSource {

    /**
     * Executes this source. Implementations should regularly check via the given context if they should stop. If that's
     * the case, they should abort their processing and perform any clean-up needed, such as rolling back pending
     * transactions, releasing locks etc.
     *
     * @param context          contextual information for this source's execution
     * @param partition        the source partition from which the snapshot should be taken
     * @param previousOffset   previous offset restored from Kafka
     * @param snapshottingTask
     * @return an indicator to the position at which the snapshot was taken
     * @throws InterruptedException in case the snapshot was aborted before completion
     */
    // 执行快照的主逻辑方法。
    // ChangeEventSourceContext context：执行上下文，用于检查任务是否被取消或暂停（通过 isRunning()）。
    // P partition：分区信息，标识快照的目标库。
    // O previousOffset：从存储中恢复的上一次位点。
    // SnapshottingTask snapshottingTask：具体的快照任务指令，定义了具体要做什么（读架构、读数据等）。
    SnapshotResult<O> execute(ChangeEventSourceContext context, P partition, O previousOffset, SnapshottingTask snapshottingTask) throws InterruptedException;

    /**
     * Returns the snapshotting task based on the previous offset (if available) and the connector's snapshotting mode.
     */
    // 根据当前状态计算“本次启动要做什么”。
    // P partition：分区信息。
    // O previousOffset：上一次的位点信息。
    // 示例：如果 previousOffset 为空且模式为 initial，它会返回一个要求“既同步架构又同步数据”的任务；如果位点已存在，可能返回一个“跳过快照”的任务。
    SnapshottingTask getSnapshottingTask(P partition, O previousOffset);

    /**
     * Returns the blocking snapshotting task based on the snapshot configuration from the signal.
     */
    // 为“阻塞式快照（也称即时快照/信号快照）”创建任务指令。
    // SnapshotConfiguration snapshotConfiguration：从信号（Signal）中解析出来的配置，例如用户通过信号表指定只需要快照某几张表。
    SnapshottingTask getBlockingSnapshottingTask(P partition, O previousOffset, SnapshotConfiguration snapshotConfiguration);
}
