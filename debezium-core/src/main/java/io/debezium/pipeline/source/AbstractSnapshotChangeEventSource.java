/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigurationDefaults;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

/**
 * An abstract implementation of {@link SnapshotChangeEventSource} that all implementations should extend
 * to inherit common functionality.
 *
 * @author Chris Cranford
 */
// AbstractSnapshotChangeEventSource 是一个至关重要的 抽象基类。
// 它实现了 SnapshotChangeEventSource 接口，为所有具体的数据库（如 MySQL、PostgreSQL、SQL Server 等）提供了 快照执行的通用工作流模板。
// 标准化快照生命周期：定义了快照从“准备阶段”到“执行阶段”再到“完成/中止阶段”的标准步骤。
// 管理辅助服务：统一处理快照过程中的 进度日志（SnapshotProgressListener） 和 通知推送（NotificationService）。
// 异常与状态处理：封装了快照失败后的日志记录和状态标记逻辑，确保在连接器重启后能正确恢复。

public abstract class AbstractSnapshotChangeEventSource<P extends Partition, O extends OffsetContext> implements SnapshotChangeEventSource<P, O>, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSnapshotChangeEventSource.class);

    /**
     * Interval for showing a log statement with the progress while scanning a single table.
     */
    // 静态常量（10秒）。
    // 用于控制扫描表时打印进度日志的时间间隔。
    public static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);
    // 通用连接器配置。
    // 用于获取快照延迟、Bean 注册表等配置信息。
    private final CommonConnectorConfig connectorConfig;
    // 快照进度监听器。
    // 负责将快照状态（开始、完成、失败、扫描行数）暴露给 JMX 度量指标。
    private final SnapshotProgressListener<P> snapshotProgressListener;
    // 通知服务。
    // 负责向外部发送快照状态通知。
    protected final NotificationService<P, O> notificationService;

    public AbstractSnapshotChangeEventSource(CommonConnectorConfig connectorConfig, SnapshotProgressListener<P> snapshotProgressListener,
                                             NotificationService<P, O> notificationService) {
        this.connectorConfig = connectorConfig;
        this.snapshotProgressListener = snapshotProgressListener;
        this.notificationService = notificationService;
    }
    // 构建一个包含当前分区和位点的包装对象。
    protected Offsets<P, OffsetContext> getOffsets(SnapshotContext<P, O> ctx, O previousOffset, SnapshottingTask snapshottingTask) {
        return Offsets.of(ctx.partition, previousOffset);
    }
    // Debezium 执行快照（Snapshot）的标准模板流程。它采用了“准备 -> 校验 -> 执行 -> 收尾”的结构。
    @Override
    public SnapshotResult<O> execute(ChangeEventSourceContext context, P partition, O previousOffset, SnapshottingTask snapshottingTask) throws InterruptedException {

        final SnapshotContext<P, O> ctx;
        // 准备阶段 (Initialization)
        try {
            // 1. 调用抽象方法 prepare，由具体的数据库子类初始化快照上下文（例如建立连接、开启事务）
            ctx = prepare(partition, snapshottingTask.isOnDemand());
            // In case of a bocking snapshot the offsets of snapshot context must be the set to avoid reinitialization
            // to an empty one during the determineSnapshotOffset function
            // 2. 如果是“按需快照”（On-Demand/Blocking Snapshot），需要继承之前的位点
            // 这样做是为了防止在确定快照位点函数中将其重置为空，确保数据的连续性
            if (previousOffset != null && snapshottingTask.isOnDemand()) {
                ctx.offset = previousOffset;
            }
            // 3. 将快照上下文注册到 Bean 注册中心，方便其他组件（如自定义过滤器）访问状态
            connectorConfig.getBeanRegistry().add(StandardBeanNames.SNAPSHOT_CONTEXT, ctx);
        }
        catch (Exception e) {
            LOGGER.error("Failed to initialize snapshot context.", e);
            throw new RuntimeException(e);
        }
        // 校验与跳过阶段 (Validation)
        // 4. 获取当前的分区和位点包装对象，用于后续的通知推送
        Offsets<P, OffsetContext> offsets = getOffsets(ctx, previousOffset, snapshottingTask);
        // 5. 根据 snapshot.mode 的配置判断是否需要跳过快照
        if (snapshottingTask.shouldSkipSnapshot()) {
            LOGGER.debug("Skipping snapshotting");
            // 更新监控指标：标记为跳过
            snapshotProgressListener.snapshotSkipped(partition);
            // 发送通知：快照已跳过
            notificationService.initialSnapshotNotificationService().notifySkipped(offsets.getTheOnlyPartition(), offsets.getTheOnlyOffset());
            return SnapshotResult.skipped(previousOffset);
        }
        // 6. 如果配置了 snapshot.delay.ms，线程会在此处阻塞等待
        delaySnapshotIfNeeded(context);

        boolean completedSuccessfully = true;
        // 核心执行阶段 (Execution)
        try {
            // 7. 更新监控指标和发送通知：快照正式开始
            snapshotProgressListener.snapshotStarted(partition);
            notificationService.initialSnapshotNotificationService().notifyStarted(offsets.getTheOnlyPartition(), offsets.getTheOnlyOffset());
            // 8. 核心步骤：调用子类实现的 doExecute()
            // 这里会执行具体的锁表、读架构、扫描数据行等重型任务
            return doExecute(context, previousOffset, ctx, snapshottingTask);
        }
        catch (InterruptedException e) {
            completedSuccessfully = false;
            LOGGER.warn("Snapshot was interrupted before completion");
            throw e;
        }
        catch (Exception t) {
            completedSuccessfully = false;
            throw new DebeziumException(t);
        }
        finally {
            LOGGER.info("Snapshot - Final stage");
            // 9. 关闭快照相关的临时资源（如释放数据库连接）
            close();

            if (completedSuccessfully) {
                // 10. 成功逻辑
                LOGGER.info("Snapshot completed");
                // 子类钩子：例如提交事务
                completed(ctx);
                // 监控指标：标记完成
                snapshotProgressListener.snapshotCompleted(partition);
                // 发送完成通知
                notificationService.initialSnapshotNotificationService().notifyCompleted(ctx.partition, ctx.offset);
            }
            else {
                // 11. 失败逻辑
                String basicWarnMessage = "Snapshot was not completed successfully";
                String finalWarnMessage = String.format("%s %s", basicWarnMessage, ", it will be re-executed upon connector restart");

                if (snapshottingTask.isOnDemand()) {
                    // In case of error blocking snapshot will not be automatically executed.
                    // 如果是按需快照失败，标记位点完成以防止死循环，且重启后不会自动触发
                    previousOffset.postSnapshotCompletion();
                    finalWarnMessage = basicWarnMessage;
                }

                LOGGER.warn(finalWarnMessage);
                // 子类钩子：例如回滚事务
                aborted(ctx);
                snapshotProgressListener.snapshotAborted(offsets.getTheOnlyPartition());
                notificationService.initialSnapshotNotificationService().notifyAborted(ctx.partition, ctx.offset);
            }
        }
    }
    // 主要用于在快照阶段计算出哪些表（或集合）需要被执行全量读取。
    // 它是实现“按需快照”或“指定表快照”功能的核心过滤逻辑。
    // <T extends DataCollectionId>: 泛型定义。在关系型数据库中，T 通常是 TableId（包含库名、表名）
    // allDataCollections: 数据库中所有候选的数据集合（通常是连接器当前监控的所有表）。
    // snapshotAllowedDataCollections: 用户定义的“允许快照的表”正则表达式集合。这个集合通常来自信号（Signal）中的配置。
    protected <T extends DataCollectionId> Stream<T> determineDataCollectionsToBeSnapshotted(final Collection<T> allDataCollections,
                                                                                                Set<Pattern> snapshotAllowedDataCollections) {
        // 如果用户没有指定特定的过滤条件（集合为空），则默认对所有监控中的表进行快照。
        if (snapshotAllowedDataCollections.isEmpty()) {
            return allDataCollections.stream();
        }
        else {
            // 开始遍历数据库中所有的表
            // 只有符合条件的表才会被保留在快照清单中。
            return allDataCollections.stream()
                    .filter(dataCollectionId -> snapshotAllowedDataCollections.stream()
                            .anyMatch(tableNameMatcher(dataCollectionId)));
        }
    }
    // 作用是创建一个判定谓词（Predicate），用于检查一个正则表达式模式（Pattern）是否匹配给定的表标识符（dataCollectionId）。
    private static <T extends DataCollectionId> Predicate<Pattern> tableNameMatcher(T dataCollectionId) {
        return s -> s.matcher(dataCollectionId.identifier()).matches() ||
                s.matcher(((TableId) dataCollectionId).toDoubleQuotedString()).matches();
    }

    /**
     * Delays snapshot execution as per the {@link CommonConnectorConfig#SNAPSHOT_DELAY_MS} parameter.
     */
    // 实现了 Debezium 连接器在启动快照前的强制等待逻辑。
    // 其主要目的是为了在某些高可用场景下，给数据库副本同步或网络初始化预留缓冲时间。
    protected void delaySnapshotIfNeeded(ChangeEventSourceContext context) throws InterruptedException {
        Duration snapshotDelay = connectorConfig.getSnapshotDelay();

        if (snapshotDelay.isZero() || snapshotDelay.isNegative()) {
            return;
        }

        Threads.Timer timer = Threads.timer(Clock.SYSTEM, snapshotDelay);
        Metronome metronome = Metronome.parker(ConfigurationDefaults.RETURN_CONTROL_INTERVAL, Clock.SYSTEM);

        while (!timer.expired()) {
            if (!context.isRunning()) {
                throw new InterruptedException("Interrupted while awaiting initial snapshot delay");
            }

            LOGGER.info("The connector will wait for {}s before proceeding", timer.remaining().getSeconds());
            metronome.pause();
        }
    }

    /**
     * Executes this source.  Implementations should regularly check via the given context if they should stop.  If
     * that's the case, they should abort their processing and perform any clean-up needed, such as rolling back
     * pending transactions, releasing locks, etc.
     *
     * @param context contextual information for this source's execution
     * @param previousOffset previous offset restored from Kafka
     * @param snapshotContext mutable context information populated throughout the snapshot process
     * @param snapshottingTask immutable information about what tasks should be performed during snapshot
     * @return an indicator to the position at which the snapshot was taken
     */
    protected abstract SnapshotResult<O> doExecute(ChangeEventSourceContext context, O previousOffset,
                                                   SnapshotContext<P, O> snapshotContext,
                                                   SnapshottingTask snapshottingTask)
            throws Exception;

    /**
     * Prepares the taking of a snapshot and returns an initial {@link SnapshotContext}.
     */
    protected abstract SnapshotContext<P, O> prepare(P partition, boolean onDemand) throws Exception;

    /**
     * Completes the snapshot, doing any required clean-up (resource disposal etc.).
     * The snapshot may have run successfully or have been aborted at this point.
     *
     */
    @Override
    public void close() {
    }

    /**
     * Completes the snapshot, doing any required clean-up (resource disposal etc.).
     * The snapshot have run successfully at this point.
     *
     * @param snapshotContext snapshot context
     */
    protected void completed(SnapshotContext<P, O> snapshotContext) {
    }

    /**
     * Completes the snapshot, doing any required clean-up (resource disposal etc.).
     * The snapshot is aborted at this point.
     *
     * @param snapshotContext snapshot context
     */
    protected void aborted(SnapshotContext<P, O> snapshotContext) throws InterruptedException {
    }
    // 主要负责将用户在配置或信号中提供的 原始表名字符串列表 转换为 预编译的正则表达式模式集合。
    protected Set<Pattern> getDataCollectionPattern(List<String> dataCollections) {
        return dataCollections.stream()
                .map(tables -> Strings.setOfRegex(tables, Pattern.CASE_INSENSITIVE))
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    /**
     * Mutable context which is populated in the course of snapshotting
     */
    public static class SnapshotContext<P extends Partition, O extends OffsetContext> implements AutoCloseable {
        public P partition;
        public O offset;

        public SnapshotContext(P partition) {
            this.partition = partition;
        }

        @Override
        public void close() throws Exception {
        }
    }

}
