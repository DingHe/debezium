/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import static io.debezium.util.Loggings.maybeRedactSensitiveData;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.SingleThreadAccess;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.converters.custom.CustomConverterServiceProvider;
import io.debezium.data.Envelope;
import io.debezium.function.LogPositionValidator;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.notification.channels.NotificationChannel;
import io.debezium.pipeline.signal.channels.SignalChannelReader;
import io.debezium.pipeline.signal.channels.process.SignalChannelWriter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.processors.PostProcessorRegistryServiceProvider;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.HistorizedDatabaseSchema;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.SnapshotLockProvider;
import io.debezium.snapshot.SnapshotQueryProvider;
import io.debezium.snapshot.SnapshotterServiceProvider;
import io.debezium.spi.snapshot.Snapshotter;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;

/**
 * Base class for Debezium's CDC {@link SourceTask} implementations. Provides functionality common to all connectors,
 * such as validation of the configuration.
 *
 * @author Gunnar Morling
 */
// BaseSourceTask 是 Debezium 框架中所有具体连接器任务（如 MySqlConnectorTask, PostgresConnectorTask）的核心基类。
// 它继承自 Kafka 的 SourceTask，并实现了 Debezium 连接器通用的生命周期管理、配置校验、错误恢复和偏移量提交逻辑。
// BaseSourceTask 的核心作用是将 Kafka Connect 的底层 API 与 Debezium 的变更事件引擎（Engine）连接起来：
// 标准化启动与停止流程：封装了 start() 和 stop() 方法，处理配置解析、脱敏和状态管理。
// 协调器管理：持有并管理 ChangeEventSourceCoordinator，这是 Debezium 真正执行快照和流式读取的“大脑”。
// 异常重试机制：实现了对 RetriableException 的捕获和基于指数退避算法的自动重启逻辑。
// 状态校验：在启动时校验数据库 Schema 历史和 Binlog/日志位点是否有效，防止数据丢失。
// 指标与统计：自动记录并打印任务处理的吞吐量、最近一次提交的偏移量等统计信息。

public abstract class BaseSourceTask<P extends Partition, O extends OffsetContext> extends SourceTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseSourceTask.class);
    private static final Duration INITIAL_POLL_PERIOD_IN_MILLIS = Duration.ofMillis(TimeUnit.SECONDS.toMillis(5));
    private static final Duration MAX_POLL_PERIOD_IN_MILLIS = Duration.ofMillis(TimeUnit.HOURS.toMillis(1));
    // 存储当前任务的配置映射
    private Configuration config;

    private List<SignalChannelReader> signalChannels;
    // 连接器任务的上下文，包含运行 ID 和配置信息。
    private CdcSourceTaskContext<? extends CommonConnectorConfig> cdcSourceTaskContext;
    // 核心作用是：在正式开始抓取数据前，验证“已记录的位点（Offset）”与“数据库架构历史（Schema History）”是否一致且可用。
    // 如果不通过此检查，说明数据链路存在断裂风险（如 Binlog 被删或 Schema 历史丢失），程序会抛出异常阻止启动。
    protected void validateSchemaHistory(CommonConnectorConfig config, LogPositionValidator logPositionValidator, Offsets<P, O> previousOffsets,
                                         DatabaseSchema schema,
                                         Snapshotter snapshotter) {
        // Debezium 支持多分区（如多个数据库），代码遍历每个分区及其对应的上次处理位点。
        for (Map.Entry<P, O> previousOffset : previousOffsets) {

            Partition partition = previousOffset.getKey();
            OffsetContext offset = previousOffset.getValue();
            // 情况 A：完全没有历史位点（全新启动）
            if (offset == null) {
                // 恢复模式检查：如果配置了“架构恢复模式（Recovery）”但找不到历史位点，会抛出异常，因为恢复模式必须依赖旧位点。
                if (snapshotter.shouldSnapshotOnSchemaError()) {
                    // We are in schema only recovery mode, use the existing redo log position
                    // would like to also verify redo log position exists, but it defaults to 0 which is technically valid
                    throw new DebeziumException("Could not find existing redo log information while attempting schema only recovery snapshot");
                }
                LOGGER.info("Connector started with no previous offset for partition '{}'", partition);
                if (schema.isHistorized()) {
                    if (((HistorizedDatabaseSchema) schema).getSchemaHistory().storageExists()) {
                        LOGGER.info("Database schema history storage was found. Connector will use the pre-existing storage. Checking settings for the same.");
                        ((HistorizedDatabaseSchema) schema).getSchemaHistory().checkStorageSettings();
                    }
                    else {
                        ((HistorizedDatabaseSchema) schema).initializeStorage();
                    }
                }
                return;
            }
            // 情况 B：快照未完成就被中断
            if (offset.isInitialSnapshotRunning()) {
                // The last offset was an incomplete snapshot and now the snapshot was disabled
                // 位点显示上次任务在做快照时崩溃了。
                if (!snapshotter.shouldSnapshotData(true, true) &&
                        !snapshotter.shouldSnapshotSchema(true, true)) {
                    // No snapshots are allowed
                    throw new DebeziumException("The connector previously stopped while taking a snapshot, but now the connector is configured "
                            + "to never allow snapshots. Reconfigure the connector to use snapshots initially or when needed.");
                }
            }
            // 情况 C：架构历史（Schema History）丢失检查
            else {

                if (schema.isHistorized() && !((HistorizedDatabaseSchema) schema).getSchemaHistory().exists()) {

                    LOGGER.warn("Database schema history was not found but was expected");

                    if (snapshotter.shouldSnapshotOnSchemaError()) {

                        LOGGER.info("The db-history topic is missing but we are in {} snapshot mode. " +
                                "Attempting to snapshot the current schema and then begin reading the redo log from the last recorded offset.",
                                snapshotter.name());
                        if (schema.isHistorized()) {
                            ((HistorizedDatabaseSchema) schema).initializeStorage();
                        }
                        return;
                    }
                    else {
                        throw new DebeziumException("The db history topic is missing. You may attempt to recover it by reconfiguring the connector to recovery.");
                    }
                }

                if (config.isLogPositionCheckEnabled()) {

                    boolean logPositionAvailable = isLogPositionAvailable(logPositionValidator, partition, offset, config);

                    if (!logPositionAvailable && snapshotter.shouldStream()) {
                        LOGGER.warn("Last recorded offset is no longer available on the server.");

                        if (snapshotter.shouldSnapshotOnDataError()) {

                            LOGGER.info("The last recorded offset is no longer available but we are in {} snapshot mode. " +
                                    "Attempting to snapshot data to fill the gap.",
                                    snapshotter.name());

                            previousOffsets.resetOffset(previousOffsets.getTheOnlyPartition());

                            return;
                        }

                        throw new DebeziumException("The connector is trying to read change stream starting at " + offset + ", but this is no longer "
                                + "available on the server. Reconfigure the connector to use a snapshot mode when needed.");
                    }
                }
            }
        }
    }
    // 检测数据库日志（如 MySQL 的 Binlog 或 Oracle 的 Redo Log）是否依然存在的逻辑入口。
    // 它是防止连接器因为找不到历史位点而产生静默错误的最后一道防线。
    public boolean isLogPositionAvailable(LogPositionValidator logPositionValidator, Partition partition, OffsetContext offsetContext, CommonConnectorConfig config) {

        if (logPositionValidator == null) {
            LOGGER.warn("Current JDBC connection implementation is not providing a log position validator implementation. The check will always be 'true'");
            return true;
        }
        return logPositionValidator.validate(partition, offsetContext, config);
    }
    // 录任务当前的运行状态（INITIAL, RUNNING, RESTARTING, STOPPED）。
    private final AtomicReference<DebeziumTaskState> state = new AtomicReference<>(DebeziumTaskState.INITIAL);

    /**
     * Used to ensure that start(), stop() and commitRecord() calls are serialized.
     */
    // 确保 start, stop, commit 等关键方法在多线程环境下的原子性，防止竞争。
    private final ReentrantLock stateLock = new ReentrantLock();
    // 重试策略，决定在发生可重试错误后等待多久再次尝试。
    private volatile ElapsedTimeStrategy restartDelay;

    /**
     * The change event source coordinator for those connectors adhering to the new
     * framework structure, {@code null} for legacy-style connectors.
     */
    // 核心协调器
    // 负责调度快照源和流式源。
    protected ChangeEventSourceCoordinator<P, O> coordinator;

    /**
     * The latest offsets that have been acknowledged by the Kafka producer. Will be
     * acknowledged with the source database in {@link BaseSourceTask#commit()}
     * (which may be a no-op depending on the connector).
     */
    // 缓存已被 Kafka 生产者确认（ACK）但尚未在源数据库侧提交的最新偏移量。
    private final Map<Map<String, ?>, Map<String, ?>> lastOffsets = new HashMap<>();
    // 可重试错误后的等待时间
    private Duration retriableRestartWait;

    private final ElapsedTimeStrategy pollOutputDelay;
    // 提供系统时间，用于计算延迟和统计。
    private final Clock clock = Clock.system();
    @SingleThreadAccess("polling thread")
    private Instant previousOutputInstant;

    @SingleThreadAccess("polling thread")
    private int previousOutputBatchSize;
    // 标记是否需要在下一次 poll() 循环中执行偏移量提交。
    protected final AtomicBoolean shouldPerformCommit = new AtomicBoolean(false);
    // 使用 SPI 加载信号通道实现（用于外部触发快照等操作）。
    private final ServiceLoader<SignalChannelReader> availableSignalChannels = ServiceLoader.load(SignalChannelReader.class);
    // 存储通知通道（如通过日志、JMX 或专用 Topic 发送任务通知）。
    private final List<NotificationChannel> notificationChannels;

    /**
     * A flag to record whether the offsets stored in the offset store are loaded for the first time.
     * This is typically used to reduce logging in case a connector like PostgreSQL reads offsets
     * not only on connector startup but repeatedly during execution time too.
     */
    private boolean offsetLoadedInPast = false;

    protected BaseSourceTask() {
        // Use exponential delay to log the progress frequently at first, but the quickly tapering off to once an hour...
        pollOutputDelay = ElapsedTimeStrategy.exponential(clock, INITIAL_POLL_PERIOD_IN_MILLIS, MAX_POLL_PERIOD_IN_MILLIS);
        previousOutputInstant = clock.currentTimeAsInstant();

        this.notificationChannels = StreamSupport.stream(ServiceLoader.load(NotificationChannel.class).spliterator(), false)
                .collect(Collectors.toList());
    }

    public abstract CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration config);
    // Kafka Connect 启动任务的入口
    // Debezium 连接器任务启动的核心逻辑。它不仅负责初始化资源，还处理了复杂的状态切换、配置验证以及故障重试准备。
    @Override
    public final void start(Map<String, String> props) {
        // 确保 Kafka Connect 注入的 SourceTaskContext 不为空
        // context 是任务与 Kafka Connect 框架交互的桥梁（用于读取偏移量等）。如果为空，说明框架运行异常，任务无法继续，直接抛出异常。
        if (context == null) {
            throw new ConnectException("Unexpected null context");
        }

        stateLock.lock();

        try {
            // 将任务状态标记为 INITIAL（初始化中）
            setTaskState(DebeziumTaskState.INITIAL);
            // 将 Kafka Connect 传入的 Map 格式配置转换为 Debezium 内部通用的 Configuration 对象，方便后续进行类型安全的读取。
            config = Configuration.from(props);
            // 执行特定连接器（如 MySQL, Postgres）的预启动逻辑
            // 抽象方法，由子类实现。它通常用来创建连接器特定的上下文对象，并生成当前运行的唯一 ID（Run ID）
            cdcSourceTaskContext = preStart(config);
            // 通过 OpenLineage 协议发射任务启动的元数据信号
            DebeziumOpenLineageEmitter.emit(
                    DebeziumOpenLineageEmitter.connectorContext(getMaskedConfigurationMap(props), connectorName(), cdcSourceTaskContext.getRunId()),
                    DebeziumTaskState.INITIAL);
            // 从配置中读取“可重试错误后的等待时间”（默认通常是 10 秒）
            retriableRestartWait = config.getDuration(CommonConnectorConfig.RETRIABLE_RESTART_WAIT, ChronoUnit.MILLIS);
            // need to reset the delay or you only get one delayed restart
            // 清空之前的延迟策略。这保证了每次手动启动或正常启动时，重试计数器和延迟逻辑都会重置。
            restartDelay = null;
            // 验证用户配置的参数是否合法
            if (!config.validateAndRecord(getAllConfigurationFields(), LOGGER::error)) {
                throw new ConnectException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
            }
            // 打印配置日志 (脱敏)
            if (LOGGER.isInfoEnabled()) {
                StringBuilder configLogBuilder = new StringBuilder("Starting " + getClass().getSimpleName() + " with configuration:");
                withMaskedSensitiveOptions(config).forEach((propName, propValue) -> {
                    configLogBuilder.append("\n   ").append(propName).append(" = ").append(propValue);
                });
                configLogBuilder.append("\n");
                LOGGER.info(configLogBuilder.toString());
            }
            try {
                // 调用子类实现的抽象 start 方法，
                // 创建并启动 ChangeEventSourceCoordinator。
                // 这是任务真正开始连接数据库、读取日志的地方。
                this.coordinator = start(config);
                // 如果协调器成功启动，将状态置为 RUNNING。
                setTaskState(DebeziumTaskState.RUNNING);
                // 通知监控系统，任务已进入正常运行状态。
                DebeziumOpenLineageEmitter.emit(
                        DebeziumOpenLineageEmitter.connectorContext(getMaskedConfigurationMap(props), connectorName(), cdcSourceTaskContext.getRunId()),
                        DebeziumTaskState.RUNNING);

            }
            catch (RetriableException e) {
                LOGGER.warn("Failed to start connector, will re-attempt during polling.", e);
                restartDelay = ElapsedTimeStrategy.constant(Clock.system(), retriableRestartWait);
                setTaskState(DebeziumTaskState.RESTARTING);
            }
        }
        finally {
            stateLock.unlock();
        }
    }

    /**
     * Returns the available signal channels.
     * <p>
     *     The signal channels are loaded using the {@link ServiceLoader} mechanism and cached for the lifetime of the task.
     * </p>
     *
     * @return list of loaded signal channels
     */
    // 主要作用是动态加载并缓存所有可用的信号读取器（SignalChannelReader）。
    // 在 Debezium 中，“信号”是一种特殊的控制机制（例如通过向特定表插入记录来触发一次临时快照）。
    // 由于信号可以存储在不同的地方（如数据库表、Kafka Topic、文件等），Debezium 使用插件化设计，
    // 通过此方法在运行时探测系统中安装了哪些信号插件。
    public List<SignalChannelReader> getAvailableSignalChannels() {
        if (signalChannels == null) {
            signalChannels = availableSignalChannels.stream().map(ServiceLoader.Provider::get).collect(Collectors.toList());
        }
        return signalChannels;
    }

    /**
     * Returns the first available signal channel writer
     *
     * @return the first available signal channel writer empty optional if not available
     */
    // 在 Debezium 的信号机制中，一个通道通常负责读取（Reader）外部指令。但某些通道同时也支持反馈或自我触发（Writer），
    // 即连接器自己向信号通道写入信息（例如，发送一个通知或更新某种状态）。
    //该方法的作用是：从所有已加载的信号通道中，找到第一个具备“写入”能力（即实现了 SignalChannelWriter 接口）的通道。
    public Optional<? extends SignalChannelWriter> getAvailableSignalChannelWriter() {
        return getAvailableSignalChannels().stream()
                .filter(SignalChannelWriter.class::isInstance)
                .map(SignalChannelWriter.class::cast)
                .findFirst();
    }
    // 主要目的是确保数据库密码、密钥等敏感信息不会以明文形式出现在日志、监控指标或元数据传输中。
    protected Configuration withMaskedSensitiveOptions(Configuration config) {
        return config.withMaskedPasswords();
    }

    protected Map<String, String> getMaskedConfigurationMap(Map<String, String> props) {
        return Configuration.from(props).withMaskedPasswords().asMap();
    }

    /**
     * Called when starting this source task.  This method can throw a {@link RetriableException} to indicate
     * that the task should attempt to retry the start later.
     *
     * @param config
     *            the task configuration; implementations should wrap it in a dedicated implementation of
     *            {@link CommonConnectorConfig} and work with typed access to configuration properties that way
     */
    protected abstract ChangeEventSourceCoordinator<P, O> start(Configuration config);

    protected abstract String connectorName();
    // 连接器任务最核心的执行逻辑，负责协调启动检查、位点提交、数据拉取、统计记录以及错误处理。
    @Override
    public final List<SourceRecord> poll() throws InterruptedException {

        try {
            // in we fail to start, return empty list and try to start next poll() method call
            // 确保任务在拉取数据前处于正常运行状态
            if (!startIfNeededAndPossible()) {
                return Collections.emptyList();
            }
            // it's safe to flush offsets here as we are in the running state
            // 将已成功处理的位点信息刷新到外部系统或源数据库
            if (shouldPerformCommit.getAndSet(false)) {
                performCommit();
            }
            // 真正去获取数据库变更数据
            // 抽象方法，由具体的子类（如 MySqlConnectorTask）实现
            final List<SourceRecord> records = doPoll();
            // 记录并周期性地在日志中打印同步进度
            logStatistics(records);
            // 如果成功拿到了数据，重置错误处理器的重试计数器。
            resetErrorHandlerRetriesIfNeeded(records);

            return records;
        }
        catch (RetriableException e) {
            stop(true);
            throw e;
        }
    }
    // 实现了 Debezium 连接器的运行指标监控与日志统计功能。
    // 它的巧妙之处在于利用了“指数退避”的思想，在保证监控可见性的同时，避免了高频率日志打印对系统性能的影响。
    protected void logStatistics(final List<SourceRecord> records) {
        // 如果列表为空或日志级别过高，直接退出，节省计算资源。
        if (records == null || !LOGGER.isInfoEnabled()) {
            return;
        }
        int batchSize = records.size();

        if (batchSize > 0) {
            // We want to log the number of records per topic...
            if (LOGGER.isDebugEnabled()) {
                // 如果开启了 DEBUG 模式，会进一步拆解这批数据分别发往了哪些 Kafka Topic。
                final Map<String, Integer> topicCounts = new LinkedHashMap<>();
                records.forEach(r -> topicCounts.merge(r.topic(), 1, Integer::sum));
                for (Map.Entry<String, Integer> topicCount : topicCounts.entrySet()) {
                    LOGGER.debug("Sending {} records to topic {}", topicCount.getValue(), topicCount.getKey());
                }
            }

            SourceRecord lastRecord = records.get(batchSize - 1);
            previousOutputBatchSize += batchSize;
            if (pollOutputDelay.hasElapsed()) {
                // We want to record the status ...
                final Instant currentTime = clock.currentTime();
                LOGGER.info("{} records sent during previous {}, last recorded offset of {} partition is {}", previousOutputBatchSize,
                        Strings.duration(Duration.between(previousOutputInstant, currentTime).toMillis()),
                        lastRecord.sourcePartition(), lastRecord.sourceOffset());

                previousOutputInstant = currentTime;
                previousOutputBatchSize = 0;
            }
        }
    }

    private void updateLastOffset(Map<String, ?> partition, Map<String, ?> lastOffset) {
        stateLock.lock();
        lastOffsets.put(partition, lastOffset);
        stateLock.unlock();
    }

    /**
     * Should be called to reset the error handler's retry counter upon a successful poll or when known
     * that the connector task has recovered from a previous failure state.
     */
    protected void resetErrorHandlerRetriesIfNeeded(List<SourceRecord> records) {
        // When a connector throws a retriable error, the task is not re-created and instead the previous
        // error handler is passed into the new error handler, propagating the retry count. This method
        // allows resetting that counter when a successful poll iteration step contains new records so that when a
        // future failure is thrown, the maximum retry count can be utilized.
        if (containsChangeDataMessages(records) && coordinator != null && coordinator.getErrorHandler().getRetries() > 0) {
            coordinator.getErrorHandler().resetRetries();
        }
    }
    // 识别一组记录中是否包含真正的“数据变更消息”。
    // 在 Debezium 中，并不是所有的 SourceRecord 都代表数据库中的增删改操作。
    // 有些记录可能是心跳（Heartbeat）、事务元数据（Transaction Metadata）或架构变更（Schema Change）消息。
    protected boolean containsChangeDataMessages(List<SourceRecord> records) {
        // 如果传入的记录列表为空，显然不包含任何变更消息，直接返回 false
        if (records == null || records.isEmpty()) {
            return false;
        }
        // 对当前批次（Batch）中的每一条记录进行检查。只要找到一条符合条件的记录，就可以判定整个批次包含变更数据。
        for (SourceRecord record : records) {
            if (record.valueSchema() != null && Envelope.isEnvelopeSchema(record.valueSchema())) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns the next batch of source records, if any are available.
     */
    protected abstract List<SourceRecord> doPoll() throws InterruptedException;

    protected abstract Optional<ErrorHandler> getErrorHandler();

    /**
     * Starts this connector in case it has been stopped after a retriable error,
     * and the backoff period has passed.
     */
    // 主要职责是：在任务因为可恢复错误（如数据库暂时连接不上）进入等待状态后，判断是否已经过了“冷却期”，并尝试自动重启。
    private boolean startIfNeededAndPossible() throws InterruptedException {
        // 获取重入锁，初始化返回值为 false（默认代表当前不可运行）
        stateLock.lock();

        boolean result = false;
        try {
            DebeziumTaskState currentState = getTaskState();
            // 如果当前状态已经是 RUNNING
            // 直接将结果设为 true。这意味着任务正常，poll() 可以继续执行后续的数据拉取动作。
            if (currentState == DebeziumTaskState.RUNNING) {
                result = true;
            }
            else if (currentState == DebeziumTaskState.RESTARTING) {
                // 发射重启元数据 (OpenLineage)
                getErrorHandler().ifPresentOrElse(
                        handler -> DebeziumOpenLineageEmitter.emit(
                                DebeziumOpenLineageEmitter.connectorContext(getMaskedConfigurationMap(config.asMap()), connectorName(), cdcSourceTaskContext.getRunId()),
                                DebeziumTaskState.RESTARTING,
                                handler.getProducerThrowable()),
                        () -> DebeziumOpenLineageEmitter.emit(
                                DebeziumOpenLineageEmitter.connectorContext(getMaskedConfigurationMap(config.asMap()), connectorName(), cdcSourceTaskContext.getRunId()),
                                DebeziumTaskState.RESTARTING));

                // we're in restart mode... check if it's time to restart
                // 检查设定的等待时间（如 10 秒）是否已到
                if (restartDelay.hasElapsed()) {
                    LOGGER.info("Attempting to restart task.");
                    // 重新运行前置准备逻辑（重新创建上下文）
                    preStart(config);
                    this.coordinator = start(config);
                    LOGGER.info("Successfully restarted task");
                    restartDelay = null;
                    setTaskState(DebeziumTaskState.RUNNING);
                    result = true;
                }
                // 未到时间：继续等待
                else {
                    LOGGER.info("Awaiting end of restart backoff period after a retriable error");
                    Metronome.parker(retriableRestartWait, Clock.SYSTEM).pause();
                }
            }
        }
        finally {
            stateLock.unlock();
        }
        return result;
    }

    @Override
    public final void stop() {
        try {
            performCommit();
        }
        catch (Exception e) {
            LOGGER.warn("Error while performing commit.", e);
        }
        finally {
            stop(false);
            DebeziumOpenLineageEmitter
                    .cleanup(DebeziumOpenLineageEmitter.connectorContext(getMaskedConfigurationMap(config.asMap()), connectorName(), cdcSourceTaskContext.getRunId()));
        }
    }

    private void stop(boolean restart) {
        stateLock.lock();

        try {
            if (restart) {
                LOGGER.warn("Going to restart connector after {} sec. after a retriable exception", retriableRestartWait.getSeconds());
            }
            else {
                LOGGER.info("Stopping down connector");
            }

            try {
                if (coordinator != null) {
                    coordinator.stop();
                    coordinator = null;
                }
            }
            catch (InterruptedException e) {
                Thread.interrupted();
                LOGGER.error("Interrupted while stopping coordinator", e);
                throw new ConnectException("Interrupted while stopping coordinator, failing the task");
            }

            doStop();

            if (restart) {
                setTaskState(DebeziumTaskState.RESTARTING);
                if (restartDelay == null) {
                    restartDelay = ElapsedTimeStrategy.constant(Clock.system(), retriableRestartWait);
                }
            }
            else {
                setTaskState(DebeziumTaskState.STOPPED);
                DebeziumOpenLineageEmitter.emit(
                        DebeziumOpenLineageEmitter.connectorContext(getMaskedConfigurationMap(config.asMap()), connectorName(), cdcSourceTaskContext.getRunId()),
                        DebeziumTaskState.STOPPED);
            }
        }
        finally {
            stateLock.unlock();
        }
    }

    protected abstract void doStop();

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
        LOGGER.trace("Committing record {}", maybeRedactSensitiveData(record));

        Map<String, ?> currentOffset = record.sourceOffset();
        if (currentOffset != null) {
            updateLastOffset(record.sourcePartition(), currentOffset);
        }
    }

    @Override
    public void commit() throws InterruptedException {
        shouldPerformCommit.set(true);
    }
    // 核心任务是将 Kafka 已经成功接收并记录的位点（Offset），同步回源数据库或 Debezium 的内部协调器中。
    public void performCommit() {
        // 尝试获取任务状态锁，但不会像 lock() 那样一直死等。
        boolean locked = stateLock.tryLock();

        if (locked) {
            try {
                if (coordinator != null) {
                    // 遍历待提交的位点
                    Iterator<Map<String, ?>> iterator = lastOffsets.keySet().iterator();
                    while (iterator.hasNext()) {
                        Map<String, ?> partition = iterator.next();
                        Map<String, ?> lastOffset = lastOffsets.get(partition);

                        LOGGER.debug("Committing offset '{}' for partition '{}'", partition, lastOffset);
                        // 调用协调器将位点通知给底层的数据库引擎
                        coordinator.commitOffset(partition, lastOffset);
                        iterator.remove();
                    }
                }
            }
            finally {
                stateLock.unlock();
            }
        }
        else {
            LOGGER.info("Couldn't commit processed log positions with the source database due to a concurrent connector shutdown or restart");
        }
    }

    /**
     * Returns all configuration {@link Field} supported by this source task.
     */
    protected abstract Iterable<Field> getAllConfigurationFields();

    /**
     * Loads the connector's persistent offsets (if present) via the given loader.
     */
    protected Offsets<P, O> getPreviousOffsets(Partition.Provider<P> provider, OffsetContext.Loader<O> loader) {
        Set<P> partitions = provider.getPartitions();
        OffsetReader<P, O, OffsetContext.Loader<O>> reader = new OffsetReader<>(
                context.offsetStorageReader(), loader);
        Map<P, O> offsets = reader.offsets(partitions);

        boolean found = false;
        for (P partition : partitions) {
            O offset = offsets.get(partition);

            if (offset != null) {
                found = true;
                if (offsetLoadedInPast) {
                    LOGGER.debug("Found previous partition offset {}: {}", partition, offset.getOffset());
                }
                else {
                    LOGGER.info("Found previous partition offset {}: {}", partition, offset.getOffset());
                    offsetLoadedInPast = true;
                }
            }
        }

        if (!found) {
            LOGGER.info("No previous offsets found");
        }

        return Offsets.of(offsets);
    }

    /**
     * Sets the new state for the task. The caller must be holding {@link #stateLock} lock.
     *
     * @param newState
     */
    private void setTaskState(DebeziumTaskState newState) {
        DebeziumTaskState oldState = state.getAndSet(newState);
        LOGGER.debug("Setting task state to '{}', previous state was '{}'", newState, oldState);
    }

    @VisibleForTesting
    public DebeziumTaskState getTaskState() {
        stateLock.lock();
        try {
            return state.get();
        }
        finally {
            stateLock.unlock();
        }
    }

    public List<NotificationChannel> getNotificationChannels() {
        return notificationChannels;
    }

    protected void registerServiceProviders(ServiceRegistry serviceRegistry) {
        serviceRegistry.registerServiceProvider(new PostProcessorRegistryServiceProvider());
        serviceRegistry.registerServiceProvider(new SnapshotLockProvider());
        serviceRegistry.registerServiceProvider(new SnapshotQueryProvider());
        serviceRegistry.registerServiceProvider(new SnapshotterServiceProvider());
        serviceRegistry.registerServiceProvider(new DebeziumHeaderProducerProvider());
        serviceRegistry.registerServiceProvider(new CustomConverterServiceProvider());
    }
}
