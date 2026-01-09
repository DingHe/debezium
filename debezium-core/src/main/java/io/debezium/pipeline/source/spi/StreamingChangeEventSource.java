/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;

import java.io.Closeable;
import java.util.Map;

import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

/**
 * A change event source that emits events from a DB log, such as MySQL's binlog or similar.
 *
 * @author Gunnar Morling
 */
// 定义了连接器如何从数据库的重做日志（Transaction Log，如 MySQL 的 Binlog、PostgreSQL 的 WAL）中连续读取变更数据的标准行为。
// 这个接口的核心作用是实现增量数据捕获（CDC 的 Streaming 阶段）。
// 在 Debezium 的生命周期中，通常先由 SnapshotChangeEventSource 完成历史存量数据的读取（快照），然后立即切换到该接口的实现类（如 MySqlStreamingChangeEventSource），开始长连接监听数据库的实时变更。

public interface StreamingChangeEventSource<P extends Partition, O extends OffsetContext>
        extends ChangeEventSource, Closeable {

    /**
     * Initializes the streaming source.
     * Called before incremental snapshot init.
     *
     * @throws InterruptedException
     */
    // 初始化流式源。
    // 在正式进入 execute() 循环之前被调用。它通常用于准备工作，比如在执行“增量快照”之前进行某些状态检查。默认不执行任何操作。
    default void init(O offsetContext) throws InterruptedException {
    }

    /**
     * Executes this source. Implementations should regularly check via the given context if they should stop. If that's
     * the case, they should abort their processing and perform any clean-up needed, such as rolling back pending
     * transactions, releasing locks etc.
     *
     * @param context
     *            contextual information for this source's execution
     * @param partition
     *            the source partition from which the changes should be streamed
     * @param offsetContext
     * @return an indicator to the position at which the snapshot was taken
     * @throws InterruptedException
     *             in case the snapshot was aborted before completion
     */
    // 启动主循环，执行增量同步逻辑。
    // context: 用于检查连接器是否应当停止。
    // partition: 当前处理的数据分区信息。
    // offsetContext: 存储了从快照阶段传承下来的位点（如 Binlog 文件和 Pos），流式读取将从这里开始。
    void execute(ChangeEventSourceContext context, P partition, O offsetContext) throws InterruptedException;

    /**
     * Executes this source for a single execution iteration. This is useful for iterating over multiple partitions and performing
     * an action if events were processed. For example, pausing a connector once no events were produced after iterating over all
     * partitions. Implementations should regularly check via the given context if they should stop. If that's
     * the case, they should abort their processing and perform any clean-up needed, such as rolling back pending
     * transactions, releasing locks etc.
     *
     * @param context
     *            contextual information for this source's execution
     * @param partition
     *            the source partition from which the changes should be streamed
     * @param offsetContext
     * @return true if events were processed during the iteration or false otherwise.
     * @throws InterruptedException
     *             in case the snapshot was aborted before completion
     */
    // 单次迭代执行
    // 与 execute 的长连接循环不同，它只执行一次拉取尝试。
    // 这对于那些需要管理多个分区、并根据是否有新数据产生来决定暂停或继续的连接器非常有用。
    // 返回值：如果本次迭代处理了事件，返回 true；否则返回 false。
    default boolean executeIteration(ChangeEventSourceContext context, P partition, O offsetContext) throws InterruptedException {
        throw new UnsupportedOperationException("Currently unsupported by the connector");
    }

    /**
     * Commits the given offset with the source database. Used by some connectors
     * like Postgres and Oracle to indicate how far the source TX log can be
     * discarded.
     */
    // 向源数据库确认（Commit）已处理的位点。
    // 并非所有数据库都需要。
    // 对于 MySQL 来说，位点通常保存在 Kafka Offset 中。但对于 PostgreSQL (Logical Decoding) 或 Oracle，
    // 需要通过此方法告知数据库：“这些日志我已经处理完了，你可以安全地回收/清理它们了”。
    default void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
    }
    // 获取当前的偏移量上下文
    // 返回流式源当前正在使用的位点对象。这在监控、度量以及状态切换时非常重要。
    default O getOffsetContext() {
        return null;
    }

    @Override
    default void close() {
    }
}
