/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.spi.snapshot;

import io.debezium.common.annotation.Incubating;
import io.debezium.spi.common.Configurable;

/**
 * {@link Snapshotter} is used to determine the following details about the snapshot process:
 * <p>
 * - Whether a snapshot occurs. <br>
 * - Whether streaming continues during the snapshot. <br>
 * - Whether the snapshot includes schema (if supported). <br>
 * - Whether to snapshot data or schema following an error.
 * <p>
 * Although Debezium provides many default snapshot modes,
 * to provide more advanced functionality, such as partial snapshots,
 * you can customize implementation of the interface.
 * For more information, see the documentation.
 *
 *
 * @author Mario Fiore Vitale
 */
// Snapshotter 是一个非常关键的 SPI（服务提供者接口）。它像是一个“决策大脑”，专门负责控制连接器在启动和运行过程中的快照行为策略。
// 核心作用是策略解耦。 Debezium 不需要将复杂的快照逻辑硬编码在每个数据库连接器中，而是通过这个接口来决定：
// 是否需要执行快照：根据偏移量（Offset）状态判断是执行全量快照、增量快照还是跳过。
// 快照的内容：是只快照表结构（Schema），还是既快照结构又快照数据（Data）。
// 流处理的衔接：快照结束后是否继续进行增量流采集（Streaming）。
// 异常恢复策略：当架构历史损坏或数据流出现断层（Gap）时，是否通过重新触发快照来修复。
@Incubating
public interface Snapshotter extends Configurable {

    /**
     * @return the name of the snapshotter.
     *
     *
     */
    // 返回快照策略器的名称。
    // 通常对应配置文件中的 snapshot.mode 的值，方便 Debezium 内部加载正确的实现类。
    String name();

    /**
     * @param offsetExists is {@code true} when the connector has an offset context (i.e. restarted)
     * @param snapshotInProgress is {@code true} when the connector is started, but a snapshot is already in progress
     *
     * @return {@code true} if the snapshotter should take a data snapshot
     */
    // offsetExists: Kafka 中是否已经存在该连接器的偏移量记录。
    // snapshotInProgress: 标记上一次快照是否正在进行中（即上次快照中途崩溃了）。
    // 决定是否执行数据内容的快照。
    boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress);

    /**
     * @param offsetExists is {@code true} when the connector has an offset context (i.e. restarted)
     * @param snapshotInProgress is {@code true} when the connector is started, but a snapshot is already in progress
     *
     * @return {@code true} if the snapshotter should take a schema snapshot
     */
    // 决定是否执行**表结构（架构）**的快照。
    boolean shouldSnapshotSchema(boolean offsetExists, boolean snapshotInProgress);

    /**
     * @return {@code true} if the snapshotter should stream after taking a snapshot
     */
    // 快照完成后，是否启动增量采集模式（binlog/CDC 实时流）。
    boolean shouldStream();

    /**
     * @return {@code true} whether the schema can be recovered if database schema history is corrupted.
     */
    // 当“架构历史主题（Schema History Topic）”出现数据损坏或丢失时，是否允许通过重新快照来恢复架构信息。
    boolean shouldSnapshotOnSchemaError();

    /**
     * @return {@code true} whether the snapshot should be re-executed when there is a gap in data stream.
     */
    // 当实时流（Streaming）检测到数据断层（例如 binlog 被数据库物理删除了），是否自动触发一次快照来弥补丢失的数据。
    boolean shouldSnapshotOnDataError();

    /**
     *
     * @return {@code true} if streaming should resume from the start of the snapshot
     * transaction, or {@code false} for when a connector resumes and takes a snapshot,
     * streaming should resume from where streaming previously left off.
     */
    // 定义流处理的起点。
    // 返回 true：流处理紧接着快照事务结束后的位点开始（最常用）。
    // 返回 false：如果连接器是重启的，流处理从上次保存的位点继续，而不是从快照点开始。
    default boolean shouldStreamEventsStartingFromSnapshot() {
        return true;
    }

    /**
     * Lifecycle hook called after the snapshot phase is successful.
     */
    // 快照成功结束后的回调。可以用于清理临时资源或发送通知。
    default void snapshotCompleted() {
        // no operation
    }

    /**
     * Lifecycle hook called after the snapshot phase is aborted.
     */
    default void snapshotAborted() {
        // no operation
    }
}
