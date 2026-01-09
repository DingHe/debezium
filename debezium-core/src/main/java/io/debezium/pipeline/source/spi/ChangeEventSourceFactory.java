/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import java.util.Optional;

import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.spi.schema.DataCollectionId;

/**
 * A factory for creating {@link ChangeEventSource}s specific to one database.
 *
 * @author Gunnar Morling
 */
// 在 Debezium 的设计哲学中，它将数据采集分为不同的阶段（快照、流式、增量快照）。
// 这个工厂接口的作用就是解耦：核心调度引擎不需要知道它是处理 MySQL 还是 PostgreSQL，只需要通过这个工厂获取对应的“执行源”即可。
// 统一对象创建入口：它是连接器（Connector）生命周期中的核心组件，负责创建处理数据变更的三大核心组件。
// 实现多数据库适配：不同的数据库（MySQL, Oracle, SQL Server 等）会实现此接口（例如 MySqlChangeEventSourceFactory），从而提供特定于数据库的事件源实现。
public interface ChangeEventSourceFactory<P extends Partition, O extends OffsetContext> {

    /**
     * Returns a snapshot change event source that may emit change events for schema and/or data changes. Depending on
     * the snapshot mode, a given source may decide to do nothing at all if a previous offset is given. In this case it
     * should return that given offset context from its
     * {@link StreamingChangeEventSource#execute(ChangeEventSource.ChangeEventSourceContext, Partition, io.debezium.pipeline.spi.OffsetContext)}
     * method.
     *
     * @param snapshotProgressListener
     *            A listener called for changes in the state of snapshot. May be {@code null}.
     *
     * @return A snapshot change event source
     */
    // 创建一个用于**快照（Snapshot）**的事件源。
    // SnapshotProgressListener：快照进度监听器，用于监控快照执行状态（如：已扫描多少行、是否完成）。
    // NotificationService：通知服务，用于在快照开始、结束或出错时发送通知。
    SnapshotChangeEventSource<P, O> getSnapshotChangeEventSource(SnapshotProgressListener<P> snapshotProgressListener, NotificationService<P, O> notificationService);

    /**
     * Returns a streaming change event source that starts streaming at the given offset.
     */
    // 创建一个用于**流式（Streaming）**增量采集的事件源。
    // CDC 的核心。在快照完成后，引擎会调用此方法获取流式执行源。
    // 对于 MySQL，它返回的就是你之前看到的 MySqlStreamingChangeEventSource。它负责长连接数据库，监听 Binlog/WAL 等变更日志。
    StreamingChangeEventSource<P, O> getStreamingChangeEventSource();

    /**
     * Returns and incremental snapshot change event source that can run in parallel with streaming
     * and read and send data collection content in chunk.
     *
     * @param offsetContext
     *            A context representing a restored offset from an earlier run of this connector. May be {@code null}.
     * @param snapshotProgressListener
     *            A listener called for changes in the state of snapshot. May be {@code null}.
     *
     * @return An incremental snapshot change event source
     */
    // 创建一个用于**增量快照（Incremental Snapshot）**的事件源。
    // 默认返回 Optional.empty()，因为并非所有数据库都支持或需要开启增量快照功能。
    // Debezium 的高级特性（基于信号信号机制的快照）。它允许在不停止实时流处理的情况下，动态地对某些表进行重新快照。它将全量数据分成一个个 Chunk（块）插入到实时流中，解决了传统快照必须停掉流处理且无法中途恢复的痛点。
    default Optional<IncrementalSnapshotChangeEventSource<P, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(O offsetContext,
                                                                                                                                  SnapshotProgressListener<P> snapshotProgressListener,
                                                                                                                                  DataChangeEventListener<P> dataChangeEventListener,
                                                                                                                                  NotificationService<P, O> notificationService) {
        return Optional.empty();
    }
}
