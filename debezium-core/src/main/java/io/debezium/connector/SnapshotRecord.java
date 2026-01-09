/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector;

import org.apache.kafka.connect.data.Struct;

/**
 * Describes whether the change record comes from snapshot and if it is the last one
 *
 * @author Jiri Pechanec
 *
 */
// 专门用于描述一条变更记录的**“生命周期状态”**。
// 当 Debezium 连接器启动时，它通常会经历两个阶段：
// 快照阶段（Snapshot Phase）：读取数据库的历史存量数据。
// 流处理阶段（Streaming Phase）：实时监听并读取数据库的增量变更日志（如 MySQL 的 binlog）。
// SnapshotRecord 的核心作用是：
// 标识来源：告诉消费者这条记录是来自历史存量的“扫描”，还是来自实时的“变更”。
// 标记边界：标识快照的开始、进行中、单表结束以及整个快照任务的结束。这对于下游应用（如数据仓库初始化）非常重要，因为它们需要知道何时存量数据导入完成。
public enum SnapshotRecord {
    /**
     * Record is from snapshot is not the last one.
     */
    // 快照记录 - 中间态
    // 当 Debezium 正在扫描一张大表（比如有 100 万行），除了第一条和最后一条，中间的 999,998 条记录都会标记为 TRUE
    // 下游影响：告诉消费者，这些是历史存量数据，处理时通常采用“覆盖”或“批量插入”逻辑。
    TRUE,
    /**
     * Record is from snapshot is the first record generated in snapshot phase.
     */
    // 整个快照的第一条
    // 是整个连接器（Connector）启动后，读取到的第一行存量数据
    // 唯一性：在一个完整的快照生命周期中，只会有一条记录被标记为 FIRST
    FIRST,
    /**
     * Record is from snapshot and the first record generated from the table, but not in the entire snapshot.
     */
    // 表级别第一条
    // 当前记录是某一张特定表在快照过程中的第一条记录
    // 如果同时同步 Products 和 Orders 表，当 Products 表扫描完开始扫描 Orders 时，Orders 表的第一条会标记为此状态。

    FIRST_IN_DATA_COLLECTION,
    /**
     * Record is from snapshot and the last record generated from the table, but not in the entire snapshot.
     */
    // 表级别最后一条
    // 当前记录是某一张特定表在快照过程中的最后一条记录
    LAST_IN_DATA_COLLECTION,
    /**
     * Record is from snapshot is the last record generated in snapshot phase.
     */
    // 整个快照的最后一条
    // 整个快照阶段的最后一行存量数据
    LAST,
    /**
     * Record is from streaming phase.
     */
    // FALSE (非快照记录/流记录)
    // 表示快照已完全结束，这是从数据库日志（Binlog/WAL）中实时捕获的变更。
    // 数据库正常运行期间，用户新产生的 INSERT、UPDATE、DELETE 操作。
    FALSE,
    /**
     * Record is from incremental snapshot window.
     */
    // 增量快照记录
    // 对应 Debezium 的高级特性——只读增量快照 (ReadOnly Incremental Snapshots)
    // 场景：连接器已经运行了半年，突然发现由于之前的配置漏掉了一张表，此时无需重启整个任务，可以通过发送信号触发对这张表的增量快照。
    INCREMENTAL;

    public static SnapshotRecord fromSource(Struct source) {
        if (source.schema().field(AbstractSourceInfo.SNAPSHOT_KEY) != null
                && io.debezium.data.Enum.LOGICAL_NAME.equals(source.schema().field(AbstractSourceInfo.SNAPSHOT_KEY).schema().name())) {
            final String snapshotString = source.getString(AbstractSourceInfo.SNAPSHOT_KEY);
            if (snapshotString != null) {
                return SnapshotRecord.valueOf(snapshotString.toUpperCase());
            }
        }
        return null;
    }

    public void toSource(Struct source) {
        if (this != SnapshotRecord.FALSE) {
            source.put(AbstractSourceInfo.SNAPSHOT_KEY, name().toLowerCase());
        }
    }
}
