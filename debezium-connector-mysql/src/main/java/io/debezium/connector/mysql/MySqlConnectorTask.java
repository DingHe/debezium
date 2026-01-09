/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import io.debezium.DebeziumException;
import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.DefaultQueueProvider;
import io.debezium.connector.binlog.BinlogEventMetadataProvider;
import io.debezium.connector.binlog.BinlogSourceTask;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.mysql.jdbc.MySqlConnection;
import io.debezium.connector.mysql.jdbc.MySqlConnectionConfiguration;
import io.debezium.connector.mysql.jdbc.MySqlFieldReaderResolver;
import io.debezium.connector.mysql.jdbc.MySqlValueConverters;
import io.debezium.document.DocumentReader;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.GuardrailValidator;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.snapshot.Snapshotter;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * The main task executing streaming from MySQL.
 * Responsible for lifecycle management of the streaming code.
 *
 * @author Jiri Pechanec
 *
 */
// 在 Kafka Connect 模型中，Connector 负责配置和分任务，而 Task 才是真正执行数据抓取逻辑的线程。
// 主要职责包括：
// 生命周期管理：负责 MySQL 连接器的启动（start）、运行（doPoll）和停止（doStop）。
// 组件装配：初始化并组合 Debezium 运行所需的各种核心组件，如 JDBC 连接、Schema 解析器、事件分发器（Dispatcher）和协调器（Coordinator）。
// 数据桥接：从 MySQL 数据库捕获变更事件（Snapshot 或 Binlog），将其放入内部队列，并最终转换为 Kafka Connect 的 SourceRecord 格式供 Kafka 使用。

public class MySqlConnectorTask extends BinlogSourceTask<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlConnectorTask.class);
    // 日志上下文名称，固定为 "mysql-connector-task"。
    private static final String CONTEXT_NAME = "mysql-connector-task";
    // 任务运行时上下文，包含共享的配置信息、过滤器等。
    private volatile MySqlTaskContext taskContext;
    // 内部变更事件队列。生产者将数据库事件放入队列，poll() 方法从中读取。
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    // 主要的 JDBC 连接，用于执行 SQL、获取表元数据等。
    private volatile BinlogConnectorConnection connection;
    // 专门用于 Bean 注册中心的 JDBC 连接。
    private volatile BinlogConnectorConnection beanRegistryJdbcConnection;
    // 错误处理器，负责处理在抓取过程中发生的 SQL 异常或其它可恢复/不可恢复错误。
    private volatile ErrorHandler errorHandler;
    // 内存中的 MySQL 数据库架构模型，负责维护表结构定义。
    private volatile MySqlDatabaseSchema schema;
    // 解析后的连接器配置对象。
    private MySqlConnectorConfig connectorConfig;

    @Override
    public String version() {
        return Module.version();
    }
    // 在正式 start 之前调用。
    // 负责初始化配置对象 connectorConfig 和上下文对象 taskContext
    @Override
    public CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration config) {

        connectorConfig = new MySqlConnectorConfig(config);
        taskContext = new MySqlTaskContext(config, connectorConfig);

        return taskContext;
    }
    // 是 Debezium MySQL 连接器启动的核心，
    // 负责从零开始组装整个 CDC（变更数据捕获）流水线。
    @Override
    public ChangeEventSourceCoordinator<MySqlPartition, MySqlOffsetContext> start(Configuration configuration) {
        final Clock clock = Clock.system();
        // 确定 Topic 命名策略（如：如何将表名映射到 Kafka Topic 名）。
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(MySqlConnectorConfig.TOPIC_NAMING_STRATEGY);
        // 初始化 Schema 名称调整器，确保生成的名称符合 Kafka 限制。
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();
        // 初始化数值转换器，处理 MySQL 特有的 Decimal、Temporal、Unsigned 等类型转换逻辑。
        final MySqlValueConverters valueConverters = getValueConverters(connectorConfig);

        // DBZ-3238: automatically set "useCursorFetch" to true when a snapshot fetch size other than the default of -1 is given
        // By default do not load whole result sets into memory
        // 针对快照阶段优化驱动参数
        final Configuration config = configuration.edit()
                .withDefault("database.responseBuffering", "adaptive")
                .withDefault("database.fetchSize", 10_000)
                .withDefault("database.useCursorFetch", connectorConfig.useCursorFetch())
                .build();
        // 创建连接工厂，用于生成 MySqlConnection 实例（封装了 JDBC 连接和 MySQL 特有的解析）。
        MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(() -> {
            final MySqlConnectionConfiguration connectionConfig = new MySqlConnectionConfiguration(config);
            return new MySqlConnection(connectionConfig, MySqlFieldReaderResolver.resolve(connectorConfig));
        });
        // 获取主 JDBC 连接
        connection = connectionFactory.mainConnection();
        // 从 Kafka Offset Storage 加载上一次任务停止时的位点（Offset）信息。
        Offsets<MySqlPartition, MySqlOffsetContext> previousOffsets = getPreviousOffsets(
                new MySqlPartition.Provider(connectorConfig, config),
                new MySqlOffsetContext.Loader(connectorConfig));
        // 获取数据库是否大小写敏感。
        final boolean tableIdCaseInsensitive = connection.isTableIdCaseSensitive();
        // Service providers
        // 注册内部服务（如自定义转换器注册表）
        registerServiceProviders(connectorConfig.getServiceRegistry());

        CustomConverterRegistry converterRegistry = connectorConfig.getServiceRegistry().tryGetService(CustomConverterRegistry.class);
        // 初始化内存中的数据库结构（Schema）模型，这会保存所有表的结构定义。
        this.schema = new MySqlDatabaseSchema(connectorConfig, valueConverters, topicNamingStrategy, schemaNameAdjuster, tableIdCaseInsensitive, converterRegistry,
                taskContext);

        // Manual Bean Registration
        // 将各种核心组件注册到 BeanRegistry，方便在不同组件间共享实例。
        beanRegistryJdbcConnection = connectionFactory.newConnection();
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, beanRegistryJdbcConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.VALUE_CONVERTER, valueConverters);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);
        final Snapshotter snapshotter = snapshotterService.getSnapshotter();
        // 1. 验证 MySQL 配置（如 Binlog 格式是否为 ROW，log_bin 是否开启）。
        validateBinlogConfiguration(snapshotter, connection);

        // If the binlog position is not available it is necessary to re-execute snapshot
        // 2. 验证快照可行性（如果 Binlog 已过期且没有 Offset，可能需要强制重做快照）。
        if (validateSnapshotFeasibility(snapshotter, previousOffsets.getTheOnlyOffset(), connection)) {
            previousOffsets.resetOffset(previousOffsets.getTheOnlyPartition());
        }

        // Validate guardrail limits for captured tables to prevent loading excessive table schemas into memory
        // 3. 校验护栏限制（如监控的表数量是否过多，防止内存撑爆）。
        if (connectorConfig.getGuardrailCollectionsMax() <= 0) {
            LOGGER.info("Guardrail validation skipped");
        }
        else {
            validateGuardrailLimits(connectorConfig, connection);
        }

        LOGGER.info("Closing connection before starting schema recovery");
        // 先关闭连接，释放校验阶段占用的资源。
        try {
            connection.close();
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }

        MySqlOffsetContext previousOffset = previousOffsets.getTheOnlyOffset();
        // 验证并恢复 Schema 历史（如果是 Historized 类型，如存放在 Kafka Topic 中的 DDL 历史）。
        validateSchemaHistory(connectorConfig, connection::validateLogPosition, previousOffsets, schema, snapshotter);

        LOGGER.info("Reconnecting after validating schema recovery");

        try {
            // 重新建立连接，用于后续的心跳或数据读取，并关闭自动提交。
            try {
                connection.execute("SELECT 1");
            }
            catch (SQLException e) {
                LOGGER.warn("Connection was dropped during schema recovery. Reconnecting...");
                try {
                    connection.close();
                }
                catch (Exception e1) {
                    // Ignore any error
                }
                connection = connectionFactory.mainConnection();
            }

            connection.setAutoCommit(false);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to reconnect after schema recovery", e);
        }

        // If the binlog position is not available it is necessary to re-execute snapshot
        if (previousOffset == null) {
            LOGGER.info("No previous offset found");
        }
        else {
            LOGGER.info("Found previous offset {}", previousOffset);
        }

        // Set up the task record queue ...
        // 初始化内部消息队列（BlockingQueue），所有抓取的事件先入队，由 poll() 消费。
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                .queueProvider(new DefaultQueueProvider<>(connectorConfig.getMaxQueueSize()))
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .buffering()
                .build();
        // 错误处理器。
        errorHandler = new MySqlErrorHandler(connectorConfig, queue, errorHandler);
        // 元数据提供者。
        final BinlogEventMetadataProvider metadataProvider = new BinlogEventMetadataProvider();
        // 初始化信号处理器（允许通过外部 SQL 信号表或 Topic 触发动态快照等操作）。
        SignalProcessor<MySqlPartition, MySqlOffsetContext> signalProcessor = new SignalProcessor<>(
                MySqlConnector.class, connectorConfig, Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsets);

        final Configuration heartbeatConfig = config;
        // 初始化核心分发器：它负责将数据库事件转换为 Kafka 记录，并处理心跳。
        final EventDispatcher<MySqlPartition, TableId> dispatcher = new EventDispatcher<>(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                connectorConfig.getTableFilters().dataCollectionFilter(),
                DataChangeEvent::new,
                null,
                metadataProvider,
                new HeartbeatFactory<>().getScheduledHeartbeat(
                        connectorConfig,
                        () -> new MySqlConnection(
                                new MySqlConnectionConfiguration(heartbeatConfig),
                                MySqlFieldReaderResolver.resolve(connectorConfig)),
                        new BinlogHeartbeatErrorHandler(),
                        queue),
                schemaNameAdjuster, signalProcessor, connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

        // Create the binary log client that will be used for streaming change events
        // 创建 mysql-binlog-connector-java 客户端，这是真正解析 MySQL 协议读取 Binlog 的库。
        final BinaryLogClient binaryLogClient = new BinaryLogClient(
                connectorConfig.getHostName(),
                connectorConfig.getPort(),
                connectorConfig.getUserName(),
                connectorConfig.getPassword());
        // 初始化指标监控（用于 JMX 监控）。
        final MySqlStreamingChangeEventSourceMetrics streamingMetrics = new MySqlStreamingChangeEventSourceMetrics(taskContext, queue, metadataProvider,
                schema::dataCollectionIds, binaryLogClient);
        // 初始化通知服务（如快照开始/结束时发送通知）。
        NotificationService<MySqlPartition, MySqlOffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);
        // 创建总协调器：它管理“快照源”和“流处理源”的生命周期和状态转换。
        ChangeEventSourceCoordinator<MySqlPartition, MySqlOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsets,
                errorHandler,
                MySqlConnector.class,
                connectorConfig,
                new MySqlChangeEventSourceFactory(
                        connectorConfig,
                        connectionFactory,
                        errorHandler,
                        dispatcher,
                        clock,
                        schema,
                        taskContext,
                        streamingMetrics,
                        queue,
                        snapshotterService,
                        binaryLogClient),
                new MySqlChangeEventSourceMetricsFactory(streamingMetrics),
                dispatcher,
                schema,
                signalProcessor,
                notificationService,
                snapshotterService);
        // 正式启动协调器（会进入 executeChangeEventSources 逻辑）。
        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    private MySqlValueConverters getValueConverters(MySqlConnectorConfig configuration) {
        return new MySqlValueConverters(
                configuration.getDecimalMode(),
                configuration.getTemporalPrecisionMode(),
                configuration.getBigIntUnsignedHandlingMode().asBigIntUnsignedMode(),
                configuration.binaryHandlingMode(),
                configuration.isTimeAdjustedEnabled() ? MySqlValueConverters::adjustTemporal : x -> x,
                configuration.getEventConvertingFailureHandlingMode(),
                configuration.getServiceRegistry());
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();
        return records.stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    @Override
    protected void doStop() {
        try {
            if (connection != null) {
                connection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC connection", e);
        }

        try {
            if (beanRegistryJdbcConnection != null) {
                beanRegistryJdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Exception while closing JDBC bean registry connection", e);
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return MySqlConnectorConfig.ALL_FIELDS;
    }

    private void validateGuardrailLimits(MySqlConnectorConfig connectorConfig, BinlogConnectorConnection connection) {
        try {
            Set<TableId> allTableIds = connection.getAllTableIds();
            GuardrailValidator validator = new GuardrailValidator(connectorConfig, schema);
            validator.validate(allTableIds);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to validate guardrail limits", e);
        }
    }

}
