/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigurationDefaults;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.signal.actions.SignalActionProvider;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.pipeline.spi.SnapshotResult.SnapshotResultStatus;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.HistorizedDatabaseSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;
import io.debezium.util.Threads;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order.
 *
 * @author Gunnar Morling
 */
// 在 Debezium 的架构中，ChangeEventSourceCoordinator 是整个数据采集流程的**“总指挥官”**。
// 它负责协调快照（Snapshot）和增量流（Streaming）两个阶段的转换，并管理信号处理、度量指标和错误恢复。
// 核心职责是管理变更事件源的生命周期和执行顺序。
// 在 CDC 过程中，通常需要先对数据库进行一次“全量快照”，然后再无缝切换到“增量流”读取（如 Binlog）。这个类确保了：
// 阶段协调：先执行快照，成功后再启动流式处理。
// 线程管理：在独立的后台线程中执行耗时的采集任务，不阻塞 Kafka Connect 的主线程。
// 状态监控：注册并更新 JMX 监控指标（快照进度、流延迟等）。
// 交互处理：启动信号处理器（Signal Processor），允许用户在运行时通过信号表或 JMX 发送指令。
@ThreadSafe
public class ChangeEventSourceCoordinator<P extends Partition, O extends OffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventSourceCoordinator.class);

    /**
     * Waiting period for the polling loop to finish. Will be applied twice, once gracefully, once forcefully.
     */
    // 存储连接器上次停止时的位点信息，用于断点续传。
    protected final Offsets<P, O> previousOffsets;
    protected final ErrorHandler errorHandler;
    // 工厂类，用于创建具体的快照源和流式源实例。
    protected final ChangeEventSourceFactory<P, O> changeEventSourceFactory;
    protected final ChangeEventSourceMetricsFactory<P> changeEventSourceMetricsFactory;
    protected final SnapshotterService snapshotterService;
    // 单线程执行器，所有的采集逻辑（快照+流）都在这个线程中按序运行。
    protected final ExecutorService executor;
    // 专门用于处理“阻塞式快照”请求的额外线程。
    private final ExecutorService blockingSnapshotExecutor;
    // 事件分发器，负责将读取到的原始数据发送到 Kafka Connect 的缓冲区。
    protected final EventDispatcher<P, ?> eventDispatcher;
    // 数据库架构缓存，记录表结构信息。
    protected final DatabaseSchema<?> schema;
    // 信号处理器，负责处理运行时的各种控制信号。
    protected final SignalProcessor<P, O> signalProcessor;
    // 通知服务，用于向外部报告连接器状态变更（如快照开始/结束）。
    protected final NotificationService<P, O> notificationService;
    protected final CommonConnectorConfig connectorConfig;
    // 标记整个协调器是否正在运行。
    private volatile boolean running;
    // 协调快照与流切换状态的内部标志，用于阻塞式快照时的状态同步。
    private volatile boolean paused;
    private volatile boolean streaming;
    // 流事件源
    protected volatile StreamingChangeEventSource<P, O> streamingSource;
    protected final ReentrantLock commitOffsetLock = new ReentrantLock();
    // 分别负责记录快照阶段和增量阶段的 JMX 数据。
    protected SnapshotChangeEventSourceMetrics<P> snapshotMetrics;
    protected StreamingChangeEventSourceMetrics<P> streamingMetrics;
    private ChangeEventSourceContext context;
    // 快照事件源
    private SnapshotChangeEventSource<P, O> snapshotSource;
    private AtomicReference<LoggingContext.PreviousContext> previousLogContext;
    private CdcSourceTaskContext taskContext;

    public ChangeEventSourceCoordinator(Offsets<P, O> previousOffsets, ErrorHandler errorHandler, Class<? extends SourceConnector> connectorType,
                                        CommonConnectorConfig connectorConfig,
                                        ChangeEventSourceFactory<P, O> changeEventSourceFactory,
                                        ChangeEventSourceMetricsFactory<P> changeEventSourceMetricsFactory, EventDispatcher<P, ?> eventDispatcher,
                                        DatabaseSchema<?> schema,
                                        SignalProcessor<P, O> signalProcessor, NotificationService<P, O> notificationService, SnapshotterService snapshotterService) {
        this.previousOffsets = previousOffsets;
        this.errorHandler = errorHandler;
        this.changeEventSourceFactory = changeEventSourceFactory;
        this.changeEventSourceMetricsFactory = changeEventSourceMetricsFactory;
        this.snapshotterService = snapshotterService;
        this.executor = Threads.newSingleThreadExecutor(connectorType, connectorConfig.getLogicalName(), "change-event-source-coordinator");
        this.blockingSnapshotExecutor = Threads.newSingleThreadExecutor(connectorType, connectorConfig.getLogicalName(), "blocking-snapshot");
        this.eventDispatcher = eventDispatcher;
        this.schema = schema;
        this.signalProcessor = signalProcessor;
        this.notificationService = notificationService;
        this.connectorConfig = connectorConfig;
    }
    // start 方法负责初始化监控指标、恢复表结构历史，并启动核心的任务执行线程。
    public synchronized void start(CdcSourceTaskContext taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                   EventMetadataProvider metadataProvider) {
        // 1. 创建一个原子引用，用于存储和恢复日志上下文（MDC），确保日志中能打印正确的 connector 名称
        previousLogContext = new AtomicReference<>();
        try {
            this.taskContext = taskContext;
            this.snapshotMetrics = changeEventSourceMetricsFactory.getSnapshotMetrics(taskContext, changeEventQueueMetrics, metadataProvider);
            this.streamingMetrics = changeEventSourceMetricsFactory.getStreamingMetrics(taskContext, changeEventQueueMetrics, metadataProvider,
                    schema::dataCollectionIds);
            running = true;

            // run the snapshot source on a separate thread so start() won't block
            // 6. 提交任务到线程池，这是 CDC 真正开始工作的核心逻辑块
            executor.submit(() -> {
                try {
                    // 7. 设置日志上下文，标记当前处于 "snapshot"（快照）阶段，后续日志会带上这个标签
                    previousLogContext.set(taskContext.configureLoggingContext("snapshot"));
                    // 8. 向 JMX 注册快照和流处理的监控指标，用户可以通过 JConsole 或 Prometheus 查看
                    snapshotMetrics.register();
                    streamingMetrics.register();
                    LOGGER.info("Metrics registered");

                    context = new ChangeEventSourceContextImpl();
                    LOGGER.info("Context created");
                    //  10. 如果数据库架构支持历史记录（如 MySQL）且历史记录文件/Topic 已存在
                    if (schema.isHistorized() && ((HistorizedDatabaseSchema) schema).getSchemaHistory().exists()) {
                        // 11. 从上一次保存的位点（previousOffsets）恢复表结构模型
                        // 这能确保 Debezium 知道旧的 Binlog 对应的是什么样的表结构
                        ((HistorizedDatabaseSchema<?>) schema).recover(previousOffsets);
                    }
                    // 12. 从工厂中获取快照事件源（实例化具体的 SnapshotSource，如 MySqlSnapshotChangeEventSource）
                    snapshotSource = changeEventSourceFactory.getSnapshotChangeEventSource(snapshotMetrics, notificationService);
                    // 13. 进入核心执行流程：先执行快照，快照完成后自动进入流处理（Streaming）
                    // 该方法内部会根据 offset 判断是跳过快照直接流处理，还是先做全量同步
                    executeChangeEventSources(taskContext, snapshotSource, previousOffsets, previousLogContext, context);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Change event source executor was interrupted", e);
                }
                catch (Throwable e) {
                    errorHandler.setProducerThrowable(e);
                }
                finally {
                    streamingConnected(false);
                }
            });
        }
        finally {
            if (previousLogContext.get() != null) {
                previousLogContext.get().restore();
            }
        }
    }

    protected void registerSignalActionsAndStartProcessor(SignalProcessor<P, O> signalProcessor, EventDispatcher<P, ? extends DataCollectionId> dispatcher,
                                                          ChangeEventSourceCoordinator<P, ?> changeEventSourceCoordinator, CommonConnectorConfig connectorConfig) {

        // Maybe this can be moved on task
        List<SignalActionProvider> actionProviders = StreamSupport.stream(ServiceLoader.load(SignalActionProvider.class).spliterator(), false)
                .collect(Collectors.toList());

        actionProviders.stream()
                .map(provider -> provider.createActions(dispatcher, changeEventSourceCoordinator, connectorConfig))
                .flatMap(e -> e.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                .forEach(signalProcessor::registerSignalAction);

        signalProcessor.start(); // this will run on a separate thread

    }

    public Optional<SignalProcessor<P, O>> getSignalProcessor(Offsets<P, O> previousOffset) {
        return Optional.ofNullable(signalProcessor);
    }

    /**
     * Returns the current streaming change event source, if available.
     *
     * Note: the streaming source may be {@code null} until streaming has been initialized.
     */
    public Optional<StreamingChangeEventSource<P, O>> getStreamingSource() {
        return Optional.ofNullable(streamingSource);
    }
    // 核心任务是：决定是否启动全量快照，并在快照结束后（或跳过快照后）平滑地切换到增量流式同步。
    protected void executeChangeEventSources(CdcSourceTaskContext taskContext, SnapshotChangeEventSource<P, O> snapshotSource, Offsets<P, O> previousOffsets,
                                             AtomicReference<LoggingContext.PreviousContext> previousLogContext, ChangeEventSourceContext context)
            throws InterruptedException {
        // 从传入的 previousOffsets 中提取当前任务负责的数据库分区（Partition）和上次记录的偏移量（Offset）
        final P partition = previousOffsets.getTheOnlyPartition();
        final O previousOffset = previousOffsets.getTheOnlyOffset();
        // 更新日志上下文（MDC），在日志中添加 snapshot 标签和分区信息，方便运维排查。
        previousLogContext.set(taskContext.configureLoggingContext("snapshot", partition));
        // 执行具体的快照逻辑
        SnapshotResult<O> snapshotResult = doSnapshot(snapshotSource, context, partition, previousOffset);
        // 如果启用了信号功能，则将快照结束后的最新位点信息同步给 SignalProcessor。
        getSignalProcessor(previousOffsets).ifPresent(s -> s.setContext(Offsets.of(partition, snapshotResult.getOffset())));
        // 作用：记录快照结果的调试日志（例如是 COMPLETED, SKIPPED 还是 ABORTED）。
        LOGGER.debug("Snapshot result {}", snapshotResult);
        // 判断是否可以进入流处理阶段
        // 只有在连接器处于 running 状态，且快照结果为“已完成（COMPLETED）”或“已跳过（SKIPPED）”时，才会继续往下走
        if (running && snapshotResult.isCompletedOrSkipped()) {
            if (snapshotResult.isCompleted()) {
                delayStreamingIfNeeded(context);
            }
            // 将日志上下文从 snapshot 切换为 streaming。此后产生的日志将标记为流处理阶段。
            previousLogContext.set(taskContext.configureLoggingContext("streaming", partition));
            // 调用 streamEvents 启动增量采集逻辑（对于 MySQL 来说就是开始读取 Binlog）。
            streamEvents(context, partition, snapshotResult.getOffset());
        }
    }

    /**
     * Delays streaming execution as per the {@link CommonConnectorConfig#STREAMING_DELAY_MS} parameter.
     */
    protected void delayStreamingIfNeeded(ChangeEventSourceContext context) throws InterruptedException {
        if (snapshotterService != null && !snapshotterService.getSnapshotter().shouldStream()) {
            return;
        }

        Duration streamingDelay = connectorConfig.getStreamingDelay();
        if (streamingDelay.isZero() || streamingDelay.isNegative()) {
            return;
        }

        Threads.Timer timer = Threads.timer(Clock.SYSTEM, streamingDelay);
        Metronome metronome = Metronome.parker(ConfigurationDefaults.RETURN_CONTROL_INTERVAL, Clock.SYSTEM);

        while (!timer.expired()) {
            if (!context.isRunning()) {
                throw new InterruptedException("Interrupted while awaiting streaming delay");
            }

            LOGGER.info("The connector will wait for {}s before initiating streaming", timer.remaining().getSeconds());
            metronome.pause();
        }
    }

    public void doBlockingSnapshot(P partition, OffsetContext offsetContext, SnapshotConfiguration snapshotConfiguration) {

        blockingSnapshotExecutor.submit(() -> {

            previousLogContext.set(taskContext.configureLoggingContext("streaming", partition));

            paused = true;
            streaming = true;

            try {

                context.waitStreamingPaused();

                previousLogContext.set(taskContext.configureLoggingContext("snapshot"));
                LOGGER.info("Starting snapshot");

                SnapshottingTask snapshottingTask = snapshotSource.getBlockingSnapshottingTask(partition, (O) offsetContext, snapshotConfiguration);
                doSnapshot(snapshotSource, context, partition, (O) offsetContext, snapshottingTask);
            }
            catch (InterruptedException e) {
                throw new DebeziumException("Blocking snapshot has been interrupted");
            }
            catch (Exception e) {
                LOGGER.warn("Error while executing requested blocking snapshot.", e);
            }
            finally {
                eventDispatcher.setEventListener(streamingMetrics);
                try {
                    resumeStreaming(partition);
                }
                catch (InterruptedException e) {
                    LOGGER.warn("Streaming resume has been interrupted");
                }
            }
        });
    }

    private void resumeStreaming(P partition) throws InterruptedException {
        previousLogContext.set(taskContext.configureLoggingContext("streaming", partition));
        paused = false;
        context.resumeStreaming();
    }
    // Debezium 执行“数据初始化阶段”的入口。
    // 虽然这段代码看起来非常简短，但它实际上完成了一个极其关键的动作：决策与分发。
    protected SnapshotResult<O> doSnapshot(SnapshotChangeEventSource<P, O> snapshotSource, ChangeEventSourceContext context, P partition, O previousOffset)
            throws InterruptedException {
        // 根据当前状态和配置，计算并生成一个“快照任务策略对象”
        SnapshottingTask snapshottingTask = snapshotSource.getSnapshottingTask(partition, previousOffset);

        return doSnapshot(snapshotSource, context, partition, previousOffset, snapshottingTask);
    }
    // 快照执行的核心控制逻辑。
    // 它不仅负责启动全量数据扫描，还处理了一个非常关键的边缘场景：追赶流式数据（Catch-up Streaming）

    protected SnapshotResult<O> doSnapshot(SnapshotChangeEventSource<P, O> snapshotSource, ChangeEventSourceContext context, P partition, O previousOffset,
                                           SnapshottingTask snapshottingTask)
            throws InterruptedException {
        // 在正式开始全量快照之前，先尝试读取一段增量日志
        // 这是 Debezium 为了减少快照期间锁持有时间的高级特性。
        // 如果配置允许，它会先通过流式方式“追赶”一部分数据，使快照开始时的数据库状态尽可能接近当前状态，从而减少一致性锁的压力
        CatchUpStreamingResult catchUpStreamingResult = executeCatchUpStreaming(context, snapshotSource, partition, previousOffset);
        // 如果执行了追赶逻辑，需要重置相关状态。
        if (catchUpStreamingResult.performedCatchUpStreaming) {
            streamingConnected(false);
            commitOffsetLock.lock();
            streamingSource = null;
            commitOffsetLock.unlock();
        }
        eventDispatcher.setEventListener(snapshotMetrics);
        // 调用具体的快照源实现（如 MySqlSnapshotChangeEventSource）来执行全量同步。
        SnapshotResult<O> snapshotResult = snapshotSource.execute(context, partition, previousOffset, snapshottingTask);
        LOGGER.info("Snapshot ended with {}", snapshotResult);

        if (snapshotResult.getStatus() == SnapshotResultStatus.COMPLETED || schema.tableInformationComplete()) {
            schema.assureNonEmptySchema();
        }
        return snapshotResult;
    }

    protected CatchUpStreamingResult executeCatchUpStreaming(ChangeEventSourceContext context,
                                                             SnapshotChangeEventSource<P, O> snapshotSource,
                                                             P partition, O previousOffset)
            throws InterruptedException {
        return new CatchUpStreamingResult(false);
    }
    // streamEvents 方法是 Debezium 进入增量数据采集阶段的终极入口。
    // 它的任务是启动长连接，实时监听数据库的变更日志（如 MySQL 的 Binlog 或 PostgreSQL 的 WAL）。
    protected void streamEvents(ChangeEventSourceContext context, P partition, O offsetContext) throws InterruptedException {
        try {
            // 执行流处理前的准备工作
            initStreamEvents(partition, offsetContext);
            // 注册信号动作并正式启动 SignalProcessor 的后台轮询线程
            getSignalProcessor(previousOffsets).ifPresent(signalProcessor -> registerSignalActionsAndStartProcessor(signalProcessor,
                    eventDispatcher, this, connectorConfig));
            // Debezium 支持某些特定的快照模式（如 initial_only 或 never）。
            // 如果配置要求“只做全量同步，不做增量订阅”，那么在快照结束后，这里会直接返回，不再启动 Binlog 监听。
            if (snapshotterService != null && !snapshotterService.getSnapshotter().shouldStream()) {
                LOGGER.info("Streaming is disabled for snapshot mode {}", snapshotterService.getSnapshotter().name());
                return;
            }

            LOGGER.info("Starting streaming");
            // 开启实时流执行（核心阻塞点）
            streamingSource.execute(context, partition, offsetContext);
            LOGGER.info("Finished streaming");
        }
        finally {
            if (streamingSource != null) {
                // Close streaming source
                streamingSource.close();
            }
        }
    }
    // 它的核心任务是实例化流处理器、绑定监控指标，并激活增量快照组件。
    protected void initStreamEvents(P partition, O offsetContext) throws InterruptedException {
        // 通过工厂类创建具体的流处理实现对象。
        // 如果是 MySQL 模式，这里会通过 MySqlChangeEventSourceFactory 创建出我们之前提到的 MySqlStreamingChangeEventSource 实例。
        // 这个对象负责后续与数据库建立 Binlog 连接。
        streamingSource = changeEventSourceFactory.getStreamingChangeEventSource();
        // 将流处理的监控指标（Metrics）注册到事件分发器中
        eventDispatcher.setEventListener(streamingMetrics);
        // 修改内部状态标识，标记流处理已进入“就绪”或“尝试连接”状态
        streamingConnected(true);
        // 将恢复出的位点（Offset）注入到流式事件源中
        streamingSource.init(offsetContext);
        // 确保信号处理器（SignalProcessor）拿到的位点是最新的
        getSignalProcessor(previousOffsets).ifPresent(s -> s.setContext(Offsets.of(partition, streamingSource.getOffsetContext())));
        // 尝试创建增量快照（Incremental Snapshot）的处理器
        final Optional<IncrementalSnapshotChangeEventSource<P, ? extends DataCollectionId>> incrementalSnapshotChangeEventSource = changeEventSourceFactory
                .getIncrementalSnapshotChangeEventSource(offsetContext, snapshotMetrics, snapshotMetrics, notificationService);
        eventDispatcher.setIncrementalSnapshotChangeEventSource(incrementalSnapshotChangeEventSource);
        // 如果增量快照组件存在，则执行其自身的初始化逻辑
        incrementalSnapshotChangeEventSource.ifPresent(x -> x.init(partition, offsetContext));
    }
    // 当 Kafka Connect 成功将数据写入 Kafka 并准备记录当前的偏移量（Offset）时，会调用此方法。
    // 其目的是让 Source Connector 有机会向数据库反馈其处理进度（例如在 MySQL 中更新 GTID 集合或在 PostgreSQL 中推进 Replication Slot）。
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        try {
            if (!commitOffsetLock.isLocked() && streamingSource != null && offset != null) {
                streamingSource.commitOffset(partition, offset);
            }
        }
        catch (Throwable e) {
            errorHandler.setProducerThrowable(e);
        }
    }

    /**
     * Stops this coordinator.
     */
    public synchronized void stop() throws InterruptedException {
        running = false;

        try {
            // Clear interrupt flag so the graceful termination is always attempted
            Thread.interrupted();
            executor.shutdown();
            blockingSnapshotExecutor.shutdown();
            final long shutdownWaitTimeout = connectorConfig.getExecutorShutdownTimeout().toMillis();
            boolean isShutdown = executor.awaitTermination(shutdownWaitTimeout, TimeUnit.MILLISECONDS);
            boolean isBlockingSnapshotShutdown = blockingSnapshotExecutor.awaitTermination(shutdownWaitTimeout, TimeUnit.MILLISECONDS);

            if (!isShutdown) {
                LOGGER.warn("Coordinator didn't stop in the expected time, shutting down executor now");

                // Clear interrupt flag so the forced termination is always attempted
                Thread.interrupted();
                executor.shutdownNow();
                executor.awaitTermination(shutdownWaitTimeout, TimeUnit.MILLISECONDS);
            }

            if (!isBlockingSnapshotShutdown) {
                LOGGER.warn("Coordinator didn't stop in the expected time, shutting down blocking snapshot executor now");

                // Clear interrupt flag so the forced termination is always attempted
                Thread.interrupted();
                blockingSnapshotExecutor.shutdownNow();
                blockingSnapshotExecutor.awaitTermination(shutdownWaitTimeout, TimeUnit.MILLISECONDS);
            }

            Optional<SignalProcessor<P, O>> processor = getSignalProcessor(previousOffsets);
            if (processor.isPresent()) {
                processor.get().stop();
            }

            if (notificationService != null) {
                notificationService.stop();
            }
            eventDispatcher.close();

            connectorConfig.getServiceRegistry().close();
        }
        finally {
            snapshotMetrics.unregister();
            streamingMetrics.unregister();
        }
    }

    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public class ChangeEventSourceContextImpl implements ChangeEventSourceContext {

        private final Lock lock = new ReentrantLock();
        private final Condition snapshotFinished = lock.newCondition();
        private final Condition streamingPaused = lock.newCondition();

        @Override
        public boolean isPaused() {
            return paused;
        }

        @Override
        public boolean isRunning() {
            return running;
        }

        @Override
        public void resumeStreaming() {
            lock.lock();
            try {
                snapshotFinished.signalAll();
                LOGGER.trace("Streaming will now resume.");
            }
            finally {
                lock.unlock();
            }
        }

        @Override
        public void waitSnapshotCompletion() throws InterruptedException {
            lock.lock();
            try {
                while (paused) {
                    LOGGER.trace("Waiting for snapshot to be completed.");
                    snapshotFinished.await();
                    streaming = true;
                }
            }
            finally {
                lock.unlock();
            }
        }

        @Override
        public void streamingPaused() {
            lock.lock();
            try {
                LOGGER.trace("Streaming paused. Blocking snapshot can now start.");
                streaming = false;
                streamingPaused.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        @Override
        public void waitStreamingPaused() throws InterruptedException {
            lock.lock();
            try {
                while (streaming) {
                    LOGGER.trace("Requested a blocking snapshot. Waiting for streaming to be paused.");
                    streamingPaused.await();
                }
            }
            finally {
                lock.unlock();
            }
        }
    }

    protected void streamingConnected(boolean status) {
        if (changeEventSourceMetricsFactory.connectionMetricHandledByCoordinator()) {
            streamingMetrics.connected(status);
            LOGGER.info("Connected metrics set to '{}'", status);
        }
    }

    protected class CatchUpStreamingResult {

        public boolean performedCatchUpStreaming;

        public CatchUpStreamingResult(boolean performedCatchUpStreaming) {
            this.performedCatchUpStreaming = performedCatchUpStreaming;
        }

    }
}
