/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import static io.debezium.pipeline.CommonOffsetContext.SNAPSHOT_COMPLETED_KEY;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Keeps track of the current offset within the source DB's change stream. This reflects in the offset as committed to
 * Kafka and in the source info block contained within CDC messages themselves.
 *
 * @author Gunnar Morling
 *
 */
// OffsetContext 的主要作用是管理和维护连接器（Connector）当前的读取位置（Offset）。
// 断点续传的关键：它记录了连接器处理到源数据库的哪个位置（如 MySQL 的 binlog pos，PostgreSQL 的 LSN）。当连接器重启时，它通过这个类恢复状态，确保数据不丢失、不重复。
// CDC 消息的“元数据源”：每条发送到 Kafka 的消息中都有一个 source 块，其内容正是由 OffsetContext 提供的。
// 状态同步器：它不仅记录位置，还记录了快照状态（是否在快照、是否快照完成）、事务信息（当前的事务 ID）以及增量快照的上下文。

public interface OffsetContext {

    /**
     * Implementations load a connector-specific offset context based on the offset values stored in Kafka.
     */
    // 用于在连接器启动时，将 Kafka 中存储的 Map 格式偏移量反序列化为具体的 OffsetContext 实现对象
    interface Loader<O extends OffsetContext> {

        // 从偏移量数据中解析快照类型
        default Optional<SnapshotType> loadSnapshot(Map<String, ?> offset) {

            Object snapshot = offset.getOrDefault(AbstractSourceInfo.SNAPSHOT_KEY, null);
            // this is to manage transition from a boolean snapshot to SnapshotType
            if (Boolean.TRUE.equals(snapshot) || Boolean.TRUE.toString().equals(snapshot)) {
                return Optional.of(SnapshotType.INITIAL);
            }

            return snapshot == null ? Optional.empty() : Optional.of(SnapshotType.valueOf((String) snapshot));
        }
        // 检查偏移量中是否标记了“快照已完成”。
        // 这对于决定重启后是继续快照还是切入流处理至关重要。
        default boolean loadSnapshotCompleted(Map<String, ?> offset) {

            return Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY)) || "true".equals(offset.get(SNAPSHOT_COMPLETED_KEY));
        }
        // 由各数据库插件实现。
        // 它负责将 Map 转换为具体的实现类（如 MySqlOffsetContext）
        O load(Map<String, ?> offset);
    }
    // 将当前的偏移量状态转换成一个标准的 Map
    // 这个 Map 会被 Kafka Connect 框架定期持久化到内部的 offsets topic 中。
    Map<String, ?> getOffset();
    // 返回 CDC 消息中 source 字段的 Schema 定义
    Schema getSourceInfoSchema();
    // 生成当前事件的 source 结构体。
    // 填充 Kafka 消息中的元数据块
    Struct getSourceInfo();

    /**
     * Whether this offset indicates that an (uncompleted) snapshot is currently running or not.
     * @return
     */
    // 判断当前是否正处于初始快照（存量数据扫描）阶段
    boolean isInitialSnapshotRunning();

    /**
     * Mark the position of the record in the snapshot.
     */
    // 在偏移量中标记当前记录在快照中的位置（如：是否是第一条、最后一条或中间记录）。
    void markSnapshotRecord(SnapshotRecord record);

    /**
     * Signals that a snapshot will begin, which should reflect in an updated offset state.
     * @param onDemand indicates whether the snapshot is initial or blocking
     */
    // 在快照开始前的预处理。更新内部状态以反映快照即将启动
    void preSnapshotStart(boolean onDemand);

    /**
     * Signals that a snapshot will complete, which should reflect in an updated offset state.
     */
    // 在快照即将完成时的信号。
    void preSnapshotCompletion();

    /**
     * Signals that a snapshot has been completed, which should reflect in an updated offset state.
     */
    // 在快照彻底完成后调用。通常用于清理快照标记并切换到实时流读取模式。
    void postSnapshotCompletion();

    /**
     * Records the name of the collection and the timestamp of the last event
     */
    // 每当处理一个变更事件时调用。
    // 记录该事件所属的表（Collection）和发生的时间戳。
    void event(DataCollectionId collectionId, Instant timestamp);

    /**
     * Provide a context used by {@link TransactionMonitor} so persist its internal state into offsets to survive
     * between restarts.
     *
     * @return transaction context
     */
    // 获取事务相关的上下文信息
    TransactionContext getTransactionContext();

    /**
     * Signals that the streaming of a batch of <i>incremental</i> snapshot events will begin,
     * which should reflect in an updated offset state.
     */
    // 指示一批增量快照事件即将开始处理
    default void incrementalSnapshotEvents() {
    }

    /**
     * Provide a context used by {@link IncrementalSnapshotChangeEventSource} so persist its internal state into offsets to survive
     * between restarts.
     *
     * @return incremental snapshot context
     */
    // 获取增量快照的专用上下文
    default IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return null;
    };
}
