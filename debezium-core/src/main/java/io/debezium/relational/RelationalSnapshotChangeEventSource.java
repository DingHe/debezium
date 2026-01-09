/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.SnapshotRecord;
import io.debezium.jdbc.CancellableResultSet;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.actions.snapshotting.AdditionalCondition;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalDatabaseConnectorConfig.SnapshotTablesRowCountOrder;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.snapshot.Snapshotter;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import io.debezium.util.Threads.Timer;

/**
 * Base class for {@link SnapshotChangeEventSource} for relational databases with or without a schema history.
 * <p>
 * A transaction is managed by this base class, sub-classes shouldn't rollback or commit this transaction. They are free
 * to use nested transactions or savepoints, though.
 *
 * @author Gunnar Morling
 */
// RelationalSnapshotChangeEventSource 是 Debezium 中一个非常核心的抽象基类，它是所有关系型数据库（如 MySQL、PostgreSQL、SQL Server、Oracle 等）执行快照逻辑的共同蓝图。
// 进一步封装了基于 JDBC 的数据库连接管理、多线程并行读取、表锁定、架构读取和数据导出等具体行为。
// 这个类的主要作用是驱动关系型数据库的快照全生命周期。它的职责包括：
// 一致性管理：通过管理 JDBC 事务和数据库锁，确保快照获取的数据在时间点上是一致的。
// 并行加速：支持多线程（Connection Pool）并行读取多张表的数据，极大提升大数据的快照效率。
// 模型构建：负责识别数据库中的表，并将其结构转化为 Debezium 的内部 Table 模型。
// 数据流转：将数据库中的物理行（Row）转换为变更事件（ChangeEvent），并通过 EventDispatcher 发送到 Kafka 管道。
// 信号处理：支持在运行过程中通过信号触发“按需快照（On-demand Snapshot）”。

public abstract class RelationalSnapshotChangeEventSource<P extends Partition, O extends OffsetContext> extends AbstractSnapshotChangeEventSource<P, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelationalSnapshotChangeEventSource.class);

    public static final Pattern SELECT_ALL_PATTERN = Pattern.compile("\\*");
    public static final Pattern MATCH_ALL_PATTERN = Pattern.compile(".*");
    // 关系型数据库的通用配置对象，包含快照模式、线程数、包含/排除表列表等。
    private final RelationalDatabaseConnectorConfig connectorConfig;
    // 主数据库连接，通常用于执行架构读取和获取元数据。
    private final JdbcConnection jdbcConnection;
    // 连接工厂，用于在多线程快照时创建额外的辅助连接。
    private final MainConnectionProvidingConnectionFactory<? extends JdbcConnection> jdbcConnectionFactory;
    // 关系数据库架构模型，存储表结构信息。
    private final RelationalDatabaseSchema schema;
    // 事件分发器，负责将生成的快照事件发送给下游。
    protected final EventDispatcher<P, TableId> dispatcher;
    // 时钟对象，用于记录事件生成的时间戳。
    protected final Clock clock;
    // 进度监听器，负责向 JMX 发送度量数据（如已扫描行数）
    private final SnapshotProgressListener<P> snapshotProgressListener;
    // 决定是否需要执行快照的服务逻辑。
    protected final SnapshotterService snapshotterService;
    // 线程安全的连接队列，用于多线程并行读取数据。
    protected Queue<JdbcConnection> connectionPool;
    // 专门用于接收信号的表 ID（如果配置了信号功能）
    private final TableId signalDataCollectionTableId;

    public RelationalSnapshotChangeEventSource(RelationalDatabaseConnectorConfig connectorConfig,
                                               MainConnectionProvidingConnectionFactory<? extends JdbcConnection> jdbcConnectionFactory,
                                               RelationalDatabaseSchema schema, EventDispatcher<P, TableId> dispatcher, Clock clock,
                                               SnapshotProgressListener<P> snapshotProgressListener, NotificationService<P, O> notificationService,
                                               SnapshotterService snapshotterService) {
        super(connectorConfig, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = jdbcConnectionFactory.mainConnection();
        this.jdbcConnectionFactory = jdbcConnectionFactory;
        this.schema = schema;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotProgressListener = snapshotProgressListener;
        this.snapshotterService = snapshotterService;

        if (!connectorConfig.getSignalingDataCollectionIds().isEmpty()) {
            this.signalDataCollectionTableId = TableId.parse(connectorConfig.getSignalingDataCollectionIds().get(0));
        }
        else {
            this.signalDataCollectionTableId = null;
        }
    }
    // doExecute 方法是 Debezium 关系型数据库快照逻辑的“总调度室”。
    // 它通过 7 个标准步骤，确保了快照的一致性、完整性和高性能。
    @Override
    public SnapshotResult<O> doExecute(ChangeEventSourceContext context, O previousOffset,
                                       SnapshotContext<P, O> snapshotContext, SnapshottingTask snapshottingTask)
            throws Exception {
        // // 1. 将泛型上下文强制转换为关系型数据库专用的上下文
        final RelationalSnapshotContext<P, O> ctx = (RelationalSnapshotContext<P, O>) snapshotContext;

        Connection connection = null;
        Throwable exceptionWhileSnapshot = null;
        try {
            // 2. 将字符串格式的表名列表编译为正则表达式 Pattern 集合，用于后续筛选
            Set<Pattern> dataCollectionsToBeSnapshotted = getDataCollectionPattern(snapshottingTask.getDataCollections());
            // 3. 获取用户自定义的查询覆盖语句（例如 snapshot.select.statement.overrides）
            Map<DataCollectionId, String> snapshotSelectOverridesByTable = snapshottingTask.getFilterQueries();
            // 4. 执行快照前的钩子方法（子类可重写，如清空临时状态）
            preSnapshot();

            LOGGER.info("Snapshot step 1 - Preparing");
            // 5. 如果发现上一次快照运行到一半中断了，打印日志提醒，本次将重新开始
            if (previousOffset != null && previousOffset.isInitialSnapshotRunning()) {
                LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
            }
            // 6. 创建主 JDBC 连接并关闭自动提交（开启长事务），调用连接创建后的钩子
            connection = createSnapshotConnection();
            connectionCreated(ctx);

            LOGGER.info("Snapshot step 2 - Determining captured tables");

            // Note that there's a minor race condition here: a new table matching the filters could be created between
            // this call and the determination of the initial snapshot position below; this seems acceptable, though
            // 7. 核心步骤：扫描数据库，确定哪些表符合过滤器条件并需要进行快照
            determineCapturedTables(ctx, dataCollectionsToBeSnapshotted, snapshottingTask);
            // 8. 向进度监听器汇报：已经确定了待监控的表列表
            snapshotProgressListener.monitoredDataCollectionsDetermined(snapshotContext.partition, ctx.capturedTables);
            // Init jdbc connection pool for reading table schema and data
            // 9. 初始化 JDBC 连接池：根据配置的线程数创建多个连接，为后续多线程读数据做准备
            connectionPool = createConnectionPool(ctx);

            LOGGER.info("Snapshot step 3 - Locking captured tables {}", ctx.capturedTables);
            // 10. 如果需要快照架构，则执行锁表（如 FLUSH TABLES WITH READ LOCK），防止读取期间架构变更
            if (snapshottingTask.snapshotSchema()) {
                lockTablesForSchemaSnapshot(context, ctx);
            }

            // In case of a bocking snapshot the offsets of snapshot context must be the set to avoid reinitialization
            // to an empty one during the determineSnapshotOffset function
            // 11. 确定快照偏移量（Offset）
            // 如果不是按需快照，则获取当前 binlog/SCN 位置。这将作为未来增量同步的起点。
            if (!snapshottingTask.isOnDemand()) {
                LOGGER.info("Snapshot step 4 - Determining snapshot offset");
                determineSnapshotOffset(ctx, previousOffset);
            }
            else {
                LOGGER.info("Snapshot step 4 - Determining snapshot offset (SKIPPED)");
            }

            LOGGER.info("Snapshot step 5 - Reading structure of captured tables");
            // 12. 调用抽象方法读取表的元数据（列名、类型、主键等）并填充到 ctx.tables 中
            readTableStructure(context, ctx, previousOffset, snapshottingTask);

            if (snapshottingTask.snapshotSchema()) {
                LOGGER.info("Snapshot step 6 - Persisting schema history");
                // 13. 为表生成“建表”事件，并将其写入 Schema History 主题中
                createSchemaChangeEventsForTables(context, ctx, snapshottingTask);

                // if we've been interrupted before, the TX rollback will cause any locks to be released
                // 14. 架构读取完毕后，立即释放架构锁（尽早减少对数据库写操作的影响）
                releaseSchemaSnapshotLocks(ctx);
            }
            else {
                LOGGER.info("Snapshot step 6 - Skipping persisting of schema history");
            }

            if (snapshottingTask.snapshotData()) {
                LOGGER.info("Snapshot step 7 - Snapshotting data");
                // 15. 并行导出核心：启动多线程，从连接池取连接，执行 SELECT 并分发数据事件
                createDataEvents(context, ctx, connectionPool, snapshotSelectOverridesByTable);
            }
            else {
                LOGGER.info("Snapshot step 7 - Skipping snapshotting of data");
                // 16. 如果不读数据（仅架构快照），则释放相关锁并标记位点
                releaseDataSnapshotLocks(ctx);
                ctx.offset.preSnapshotCompletion();
                ctx.offset.postSnapshotCompletion();
            }
            // 17. 执行快照后的钩子，发送心跳事件，确保 Offset 能够提交
            postSnapshot();
            dispatcher.alwaysDispatchHeartbeatEvent(ctx.partition, ctx.offset);
            return SnapshotResult.completed(ctx.offset);
        }
        catch (final Exception | AssertionError e) {
            LOGGER.error("Error during snapshot", e);
            exceptionWhileSnapshot = e;
            throw e;
        }
        finally {
            try {
                // 19. 依次关闭连接池中所有的额外 JDBC 连接
                if (connectionPool != null) {
                    for (JdbcConnection conn : connectionPool) {
                        if (!jdbcConnection.equals(conn)) {
                            conn.close();
                        }
                    }
                }
                rollbackTransaction(connection);
            }
            catch (final Exception e) {
                LOGGER.error("Error in finally block", e);
                if (exceptionWhileSnapshot != null) {
                    e.addSuppressed(exceptionWhileSnapshot);
                }
                throw e;
            }
        }
    }
    // 实现**并行快照（Parallel Snapshot）**的核心。
    // 它的作用是根据配置创建多个数据库连接，以便在快照阶段同时读取多张表的数据。
    private Queue<JdbcConnection> createConnectionPool(final RelationalSnapshotContext<P, O> ctx) throws SQLException {
        Queue<JdbcConnection> connectionPool = new ConcurrentLinkedQueue<>();
        // 2. 将已经存在的主连接（jdbcConnection）放入池中作为第一个可用的连接
        connectionPool.add(jdbcConnection);
        // 3. 计算实际需要的最大线程数。
        // 逻辑是：在 (用户配置的 snapshot.max.threads) 和 (实际待采集的表数量) 之间取最小值，且至少为 1。
        // 这样可以避免当只有 2 张表时却创建 10 个连接造成的资源浪费。
        int snapshotMaxThreads = Math.max(1, Math.min(connectorConfig.getSnapshotMaxThreads(), ctx.capturedTables.size()));
        // // 4. 如果计算出的线程数大于 1，则开始创建额外的辅助连接
        if (snapshotMaxThreads > 1) {
            // 5. 获取第一个快照查询语句（通常用于在某些数据库中初始化一致性会话）
            Optional<String> firstQuery = getSnapshotConnectionFirstSelect(ctx, ctx.capturedTables.iterator().next());
            // 6. 循环创建剩下的连接（从 1 开始，因为 0 号位置已经是主连接）
            for (int i = 1; i < snapshotMaxThreads; i++) {
                // 7. 使用连接工厂创建一个新连接，并强制关闭自动提交（开启事务模式）
                JdbcConnection conn = jdbcConnectionFactory.newConnection().setAutoCommit(false);
                // 8. 关键步骤：同步事务隔离级别
                // 确保新创建的连接与主连接具有完全相同的隔离级别（如 REPEATABLE READ），
                // 这样所有线程在同一时刻看到的数据快照版本才能保持一致。
                conn.connection().setTransactionIsolation(jdbcConnection.connection().getTransactionIsolation());
                // 9. 调用钩子方法，允许子类在连接创建后执行特定操作（如设置会话变量）
                connectionPoolConnectionCreated(ctx, conn);
                connectionPool.add(conn);
                // 11. 如果存在初始化查询语句，则在新连接上立即执行
                // 例如在 MySQL 中，这可能用于确保新连接进入一致性读的状态
                if (firstQuery.isPresent()) {
                    conn.execute(firstQuery.get());
                }
            }
        }

        LOGGER.info("Created connection pool with {} threads", snapshotMaxThreads);
        return connectionPool;
    }

    public Connection createSnapshotConnection() throws SQLException {

        if (!jdbcConnection.isValid()) {
            jdbcConnection.reconnect();
        }

        Connection connection = jdbcConnection.connection();
        connection.setAutoCommit(false);
        return connection;
    }
    // Debezium 处理**增量快照（Incremental Snapshot）或按需快照（On-demand Snapshot）**的核心入口。
    // 当用户通过“信号表”或 Kafka 信号发送快照指令时，系统会调用此方法来定义具体的执行计划。
    // 根据信号提供的配置信息，创建一个临时的快照任务。
    @Override
    public SnapshottingTask getBlockingSnapshottingTask(P partition, O previousOffset, SnapshotConfiguration snapshotConfiguration) {
        // 获取用户在信号中定义的附加条件列表（例如："additional-condition": "color='blue'"）
        Map<DataCollectionId, String> filtersByTable = snapshotConfiguration.getAdditionalConditions().stream()
                .collect(Collectors.toMap(k -> TableId.parse(k.getDataCollection().toString()), AdditionalCondition::getFilter));
        // snapshotSchema = true (第一个参数)：表示必须快照架构。在读取数据前，连接器会先更新表的结构信息，确保事件的 Schema 是准确的。
        // snapshotData = true (第二个参数)：表示必须快照数据。这是按需快照的核心目的——重新读取表中的行记录。
        // dataCollections (第三个参数)：传入用户在信号中指定的表名列表（如 ["table1", "table2"]）。
        // filtersByTable (第四个参数)：传入刚才解析好的表级过滤映射表，确保快照只读取符合条件的数据行。
        // onDemand = true (第五个参数)： 它告诉快照引擎：这是一次“中途插入”的任务，而不是启动时的初始化快照。引擎据此会跳过重置 binlog 位点等破坏性操作。
        return new SnapshottingTask(true, true, snapshotConfiguration.getDataCollections(), filtersByTable, true);
    }
    // 核心作用是：根据当前的偏移量（Offset）状态和用户配置，决定本次运行是否需要执行快照，以及是执行全量快照还是仅架构快照。
    public SnapshottingTask getSnapshottingTask(P partition, O previousOffset) {
        // 获取当前的 Snapshotter 实例。
        // 背景：Debezium 支持多种快照模式（如 initial, always, never, schema_only）。
        // Snapshotter 封装了这些模式的逻辑，用来判断在特定位点状态下是否该执行快照。
        final Snapshotter snapshotter = snapshotterService.getSnapshotter();
        // 从配置（snapshot.include.list 等）中获取需要执行快照的表名列表。
        List<String> dataCollectionsToBeSnapshotted = connectorConfig.getDataCollectionsToBeSnapshotted();
        // 获取用户自定义的快照查询语句（snapshot.select.statement.overrides），允许用户在快照时使用自定义的 WHERE 条件。
        Map<DataCollectionId, String> snapshotSelectOverridesByTable = connectorConfig.getSnapshotSelectOverridesByTable();
        // 分析当前偏移量状态
        boolean offsetExists = previousOffset != null;
        boolean snapshotInProgress = false;
        // offsetExists：如果为 false，说明这是连接器第一次启动。
        if (offsetExists) {
            // 检查之前的位点记录中，"快照是否正在运行" 的标记是否为 true
            snapshotInProgress = previousOffset.isInitialSnapshotRunning();
        }
        // 日志记录已完成状态
        if (offsetExists && !previousOffset.isInitialSnapshotRunning()) {
            LOGGER.info("A previous offset indicating a completed snapshot has been found.");
        }
        // 决定是否快照架构和数据
        boolean shouldSnapshotSchema = snapshotter.shouldSnapshotSchema(offsetExists, snapshotInProgress);
        boolean shouldSnapshotData = snapshotter.shouldSnapshotData(offsetExists, snapshotInProgress);

        if (shouldSnapshotData && shouldSnapshotSchema) {
            LOGGER.info("According to the connector configuration both schema and data will be snapshot.");
        }
        else if (shouldSnapshotSchema) {
            LOGGER.info("According to the connector configuration only schema will be snapshot.");
        }

        return new SnapshottingTask(shouldSnapshotSchema, shouldSnapshotData,
                dataCollectionsToBeSnapshotted, snapshotSelectOverridesByTable,
                false);
    }

    /**
     * Executes steps which have to be taken just after the database connection is created.
     */
    protected void connectionCreated(RelationalSnapshotContext<P, O> snapshotContext) throws Exception {
    }

    /**
     * Executes steps which have to be taken just after a connection pool connection is created.
     */
    protected void connectionPoolConnectionCreated(RelationalSnapshotContext<P, O> snapshotContext, JdbcConnection connection) throws SQLException {
    }

    protected List<Pattern> getSignalDataCollectionPattern(String signalingDataCollection) {
        return Strings.listOfRegex(signalingDataCollection, Pattern.CASE_INSENSITIVE);
    }

    private Stream<TableId> toTableIds(Set<TableId> tableIds, Pattern pattern) {
        return tableIds
                .stream()
                .filter(tid -> pattern.asMatchPredicate().test(connectorConfig.getTableIdMapper().toString(tid)))
                .sorted();
    }
    // 确保待快照的表清单中包含了用于信号通知的表，并且严格按照配置中的顺序对这些表进行排序。
    // 在关系型数据库中，快照的执行顺序有时会影响到数据库锁的持有时间或日志输出的逻辑，因此保持顺序的一致性非常重要。
    private Set<TableId> addSignalingCollectionAndSort(Set<TableId> capturedTables) {
        // 从配置中获取用户定义的 table.include.list（包含表列表）原始字符串。
        String tableIncludeList = connectorConfig.tableIncludeList();
        // 获取配置中定义的用于“信号（Signaling）”的数据集合 ID（通常是专门的一张表，用于存放 CDC 的控制指令）。
        List<String> signalingDataCollections = connectorConfig.getSignalingDataCollectionIds();

        List<Pattern> captureTablePatterns = new ArrayList<>();
        // 如果包含列表不为空，则将配置的字符串解析为不区分大小写的正则表达式列表
        if (!Strings.isNullOrBlank(tableIncludeList)) {
            captureTablePatterns.addAll(Strings.listOfRegex(tableIncludeList, Pattern.CASE_INSENSITIVE));
        }
        else {
            // 如果包含列表为空，则添加一个“匹配所有”的正则 (.*)，表示捕获所有库表
            captureTablePatterns.add(MATCH_ALL_PATTERN);
        }
        // 强制加入信号表模式
        // 信号表通常不在普通的数据同步清单里，但 Debezium 需要在快照期间识别它，以便处理快照过程中的各种信号指令。
        for (String signalingDataCollection : signalingDataCollections) {
            captureTablePatterns.addAll(getSignalDataCollectionPattern(signalingDataCollection));
        }

        return captureTablePatterns
                .stream()
                .flatMap(pattern -> toTableIds(capturedTables, pattern)) // // 2. 核心：按正则顺序匹配
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
    // 主要职责是根据用户配置、过滤器以及任务类型（初始化快照或按需快照），精确计算出本次快照需要采集“结构（Schema）”和“数据（Data）”的表清单。

    private void determineCapturedTables(RelationalSnapshotContext<P, O> ctx, Set<Pattern> dataCollectionsToBeSnapshotted, SnapshottingTask snapshottingTask)
            throws Exception {
        // // 1. 调用数据库驱动获取当前 catalog/schema 下的所有物理表 ID
        Set<TableId> allTableIds = getAllTableIds(ctx);
        // // 2. 根据快照任务中指定的模式（dataCollectionsToBeSnapshotted），从全量表中初步筛选出需要快照的表
        Set<TableId> snapshottedTableIds = determineDataCollectionsToBeSnapshotted(allTableIds, dataCollectionsToBeSnapshotted).collect(Collectors.toSet());

        Set<TableId> capturedTables = new HashSet<>();
        Set<TableId> capturedSchemaTables = new HashSet<>();
        // // 3. 遍历数据库中所有的表，决定哪些表需要记录其 Schema（表结构）
        for (TableId tableId : allTableIds) {
            // 检查表是否符合“架构过滤器”条件，且当前不是“按需快照”模式
            if (connectorConfig.getTableFilters().eligibleForSchemaDataCollectionFilter().isIncluded(tableId) && !snapshottingTask.isOnDemand()) {
                LOGGER.info("Adding table {} to the list of capture schema tables", tableId);
                capturedSchemaTables.add(tableId);
            }
        }
        // 确定“数据捕获”范围 (capturedTables)
        for (TableId tableId : snapshottedTableIds) {
            if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Adding table {} to the list of captured tables for which the data will be snapshotted", tableId);
                capturedTables.add(tableId);
            }
            else {
                LOGGER.trace("Ignoring table {} for data snapshotting as it's not included in the filter configuration", tableId);
            }
        }
        // 排序、注入信号表并存入上下文
        ctx.capturedTables = addSignalingCollectionAndSort(capturedTables);
        ctx.capturedSchemaTables = snapshottingTask.isOnDemand() ? ctx.capturedTables
                : capturedSchemaTables
                        .stream()
                        .sorted()
                        .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Returns all candidate tables; the current filter configuration will be applied to the result set, resulting in
     * the effective set of captured tables.
     */
    protected abstract Set<TableId> getAllTableIds(RelationalSnapshotContext<P, O> snapshotContext) throws Exception;

    /**
     * Locks all tables to be captured, so that no concurrent schema changes can be applied to them.
     */
    protected abstract void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                                        RelationalSnapshotContext<P, O> snapshotContext)
            throws Exception;

    /**
     * Determines the current offset (MySQL binlog position, Oracle SCN etc.), storing it into the passed context
     * object. Subsequently, the DB's schema (and data) will be be read at this position. Once the snapshot is
     * completed, a {@link StreamingChangeEventSource} will be set up with this initial position to continue with stream
     * reading from there.
     */
    protected abstract void determineSnapshotOffset(RelationalSnapshotContext<P, O> snapshotContext, O previousOffset)
            throws Exception;

    /**
     * Reads the structure of all the captured tables, writing it to {@link RelationalSnapshotContext#tables}.
     */
    protected abstract void readTableStructure(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<P, O> snapshotContext, O offsetContext, SnapshottingTask snapshottingTask)
            throws Exception;

    /**
     * Releases all locks established in order to create a consistent schema snapshot.
     */
    protected abstract void releaseSchemaSnapshotLocks(RelationalSnapshotContext<P, O> snapshotContext)
            throws Exception;

    /**
     * Releases all locks established in order to create a consistent data snapshot.
     */
    protected void releaseDataSnapshotLocks(RelationalSnapshotContext<P, O> snapshotContext) throws Exception {
    }
    // 主要作用是将快照阶段读取到的表结构（Schema）持久化到 Schema History Topic 中
    // 在 Debezium 中，即便是在做全量数据读取之前，也必须先记录表的“建表语句（DDL）”，这样下游消费者才能知道如何解析后续发送的二进制数据。
    protected void createSchemaChangeEventsForTables(ChangeEventSourceContext sourceContext,
                                                     RelationalSnapshotContext<P, O> snapshotContext,
                                                     SnapshottingTask snapshottingTask)
            throws Exception {
        // 1. 这是一个生命周期钩子，确保快照上下文状态标记为“已开始”
        tryStartingSnapshot(snapshotContext);
        // 2. 检查当前数据库架构是否支持历史记录（Historized）
        // 如果数据库不支持架构历史（比如某些简单的源），则不需要发送架构变更事件，直接返回
        if (!schema.isHistorized()) {
            return;
        }
        // 3. getTablesForSchemaChange 获取本次快照中所有需要记录架构的 TableId 集合
        for (Iterator<TableId> iterator = getTablesForSchemaChange(snapshotContext).iterator(); iterator.hasNext();) {
            final TableId tableId = iterator.next();
            // 4. 协作式中断检查：如果任务已经被外部停止，抛出异常退出
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while capturing schema of table " + tableId);
            }

            LOGGER.info("Capturing structure of table {}", tableId);
            // 5. 更新当前偏移量中的事件标记，关联当前的 TableId 和系统时钟
            snapshotContext.offset.event(tableId, getClock().currentTime());

            // If data are not snapshotted then the last schema change must set last snapshot flag
            // 6. 边界处理：如果本次任务【只快照架构而不快照数据】，那么在处理最后一张表时，
            // 必须将偏移量标记为“快照最后一条记录（last_snapshot_record = true）”
            if (!snapshottingTask.snapshotData() && !iterator.hasNext()) {
                lastSnapshotRecord(snapshotContext);
            }
            // 7. 从快照上下文的内存缓存（tables）中获取该表的物理模型（列名、类型、约束等）
            final Table table = snapshotContext.tables.forTable(tableId);
            // 8. 健壮性检查：如果找不到模型，说明之前的读取阶段（readTableStructure）漏掉了这张表
            if (table == null) {
                throw new DebeziumException("Unable to find relational table model for '" + tableId +
                        "', there may be an issue with your include/exclude list configuration.");
            }
            // 9. 将内存中的 Table 对象转换成一个 SchemaChangeEvent（内部通常包含 CREATE TABLE 逻辑）
            SchemaChangeEvent event = getCreateTableEvent(snapshotContext, table);
            // 10. 检查是否需要跳过该事件
            // 某些数据库（如 MySQL）如果检测到该表的架构已经完全一致地存在于历史记录中，可能会选择跳过
            if (HistorizedRelationalDatabaseSchema.class.isAssignableFrom(schema.getClass()) &&
                    ((HistorizedRelationalDatabaseSchema) schema).skipSchemaChangeEvent(event)) {
                continue;
            }
            // 11. 调用分发器（dispatcher）将架构变更事件发送出去
            // 这步操作会将 DDL 信息写入 Kafka 的 Schema History Topic，并通知内存中的 Schema 注册表更新
            dispatcher.dispatchSchemaChangeEvent(snapshotContext.partition, snapshotContext.offset, tableId, (receiver) -> {
                try {
                    receiver.schemaChangeEvent(event);
                }
                catch (Exception e) {
                    throw new DebeziumException(e);
                }
            });
        }
    }

    protected Collection<TableId> getTablesForSchemaChange(RelationalSnapshotContext<P, O> snapshotContext) {
        return snapshotContext.capturedTables;
    }

    /**
     * Creates a {@link SchemaChangeEvent} representing the creation of the given table.
     */
    protected abstract SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<P, O> snapshotContext,
                                                             Table table)
            throws Exception;

    // 负责从数据库表中拉取实际数据行并将其转换为 Kafka 事件。该方法通过多线程连接池实现了高效的并行数据采集。
    private void createDataEvents(ChangeEventSourceContext sourceContext,
                                  RelationalSnapshotContext<P, O> snapshotContext,
                                  Queue<JdbcConnection> connectionPool, Map<DataCollectionId, String> snapshotSelectOverridesByTable)
            throws Exception {
        // 1. 标记快照开始的生命周期钩子
        tryStartingSnapshot(snapshotContext);
        // 2. 获取快照事件接收器，用于处理和发送生成的数据事件
        SnapshotReceiver<P> snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();
        // 3. 根据传入的连接池大小确定快照线程数
        int snapshotMaxThreads = connectionPool.size();
        LOGGER.info("Creating snapshot worker pool with {} worker thread(s)", snapshotMaxThreads);
        // 4. 创建固定线程池和 CompletionService，用于管理并行任务的提交和执行结果
        ExecutorService executorService = Executors.newFixedThreadPool(snapshotMaxThreads);
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

        Map<TableId, String> queryTables = new HashMap<>();
        Map<TableId, OptionalLong> rowCountTables = new LinkedHashMap<>();
        // 5. 遍历所有待采集的表，为每一张表确定 SELECT 查询语句和预估行数
        for (TableId tableId : snapshotContext.capturedTables) {
            // 确定查询语句（会考虑用户在配置中的 overrides）
            final Optional<String> selectStatement = determineSnapshotSelect(snapshotContext, tableId, snapshotSelectOverridesByTable);
            if (selectStatement.isPresent()) {
                LOGGER.info("For table '{}' using select statement: '{}'", tableId, selectStatement.get());
                queryTables.put(tableId, selectStatement.get());
                // 6. 获取表的总行数（用于进度监控和后续排序）
                final OptionalLong rowCount = rowCountForTable(tableId);
                rowCountTables.put(tableId, rowCount);
            }
            else {
                // 如果无法生成查询语句（可能是过滤器导致），则跳过并通知进度监听器
                LOGGER.warn("For table '{}' the select statement was not provided, skipping table", tableId);
                snapshotProgressListener.dataCollectionSnapshotCompleted(snapshotContext.partition, tableId, 0);
            }
        }
        // 7. 如果配置了根据行数排序（升序或降序），则重新排列 rowCountTables
        // 这样可以先处理大表或先处理小表，优化整体快照时间
        if (connectorConfig.snapshotOrderByRowCount() != SnapshotTablesRowCountOrder.DISABLED) {
            LOGGER.info("Sort tables by row count '{}'", connectorConfig.snapshotOrderByRowCount());
            final var orderFactor = (connectorConfig.snapshotOrderByRowCount() == SnapshotTablesRowCountOrder.ASCENDING) ? 1 : -1;
            rowCountTables = rowCountTables.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue((a, b) -> orderFactor * Long.compare(a.orElse(0), b.orElse(0))))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        }
        // 8. 为每个线程准备独立的 Offset 副本
        // 因为每个线程处理不同的表，虽然它们逻辑上位点相同，但需要独立的实例来追踪各自的任务状态
        Queue<O> offsets = new ConcurrentLinkedQueue<>();
        offsets.add(snapshotContext.offset);
        for (int i = 1; i < snapshotMaxThreads; i++) {
            offsets.add(copyOffset(snapshotContext));
        }

        try {
            int tableCount = rowCountTables.size();
            int tableOrder = 1;
            final Set<TableId> rowCountTablesKeySet = Collections.unmodifiableSet(new HashSet<>(rowCountTables.keySet()));
            // 9. 为每一张表创建一个 Callable 任务并提交给线程池
            for (TableId tableId : rowCountTables.keySet()) {
                // 判断是否是单线程模式下的首表或末表（用于特殊标记）
                boolean firstTable = tableOrder == 1 && snapshotMaxThreads == 1;
                boolean lastTable = tableOrder == tableCount && snapshotMaxThreads == 1;
                String selectStatement = queryTables.get(tableId);
                OptionalLong rowCount = rowCountTables.get(tableId);
                // 10. 创建实际的数据采集逻辑（包含 JDBC 查询和事件转换）
                Callable<Void> callable = createDataEventsForTableCallable(sourceContext, snapshotContext, snapshotReceiver,
                        snapshotContext.tables.forTable(tableId), firstTable, lastTable, tableOrder++, tableCount, selectStatement, rowCount, rowCountTablesKeySet,
                        connectionPool, offsets);
                // 11. 提交到线程池执行
                completionService.submit(callable);
            }
            // 12. 阻塞等待所有表的快照任务执行完毕
            for (int i = 0; i < tableCount; i++) {
                completionService.take().get();
            }
        }
        finally {
            // 13. 无论成功失败，确保关闭线程池
            executorService.shutdownNow();
        }
        // 14. 数据读取完成，立即释放数据库中的数据快照锁（如有）
        releaseDataSnapshotLocks(snapshotContext);
        // 15. 遍历所有 Offset 副本，执行快照完成前的预处理
        for (O offset : offsets) {
            offset.preSnapshotCompletion();
        }
        // 16. 通知接收器：整个快照过程已完成
        snapshotReceiver.completeSnapshot();
        // 17. 执行快照完成后的清理工作（如在位点中标记 snapshot=false，进入增量流阶段）
        for (O offset : offsets) {
            offset.postSnapshotCompletion();
        }
    }

    protected abstract O copyOffset(RelationalSnapshotContext<P, O> snapshotContext);

    protected void tryStartingSnapshot(RelationalSnapshotContext<P, O> snapshotContext) {
        if (!snapshotContext.offset.isInitialSnapshotRunning()) {
            snapshotContext.offset.preSnapshotStart(snapshotContext.onDemand);
        }
    }

    /**
     * For the given table gets source.ts_ms value from the database for snapshot data!
     * For Postgresql its globally static for all tables since postgresql snapshot process setting auto commit off.
     * For Mysql its static per table and might be ~second behind of the select statements start ts.
     */
    protected Instant getSnapshotSourceTimestamp(JdbcConnection jdbcConnection, O offset, TableId tableId) {
        try {
            Optional<Instant> snapshotTs = jdbcConnection.getCurrentTimestamp();
            if (snapshotTs.isEmpty()) {
                throw new ConnectException("Failed reading CURRENT_TIMESTAMP from source database");
            }

            return snapshotTs.get();
        }
        catch (SQLException e) {
            throw new ConnectException("Failed reading CURRENT_TIMESTAMP from source database", e);
        }
    }
    // Debezium 多线程快照机制的执行单元封装器。
    // 它的作用是为每一张表创建一个可以并行执行的任务（Callable），并管理该任务所需的资源（数据库连接和偏移量状态）。
    // 该方法本身不执行快照，而是返回一个 Callable 对象。
    protected Callable<Void> createDataEventsForTableCallable(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<P, O> snapshotContext,
                                                              SnapshotReceiver<P> snapshotReceiver, Table table, boolean firstTable, boolean lastTable, int tableOrder,
                                                              int tableCount, String selectStatement, OptionalLong rowCount, Set<TableId> rowCountTablesKeySet,
                                                              Queue<JdbcConnection> connectionPool, Queue<O> offsets) {
        return () -> {
            // 1. 从并发队列中弹出一个空闲的 JDBC 连接
            JdbcConnection connection = connectionPool.poll();
            // 2. 从并发队列中弹出一个偏移量副本（Offset）
            O offset = offsets.poll();
            try {
                // 3. 调用实际的执行方法，进行 SQL 查询、数据读取和事件发送
                doCreateDataEventsForTable(sourceContext, snapshotContext, offset, snapshotReceiver, table, firstTable, lastTable, tableOrder, tableCount,
                        selectStatement, rowCount, rowCountTablesKeySet, connection);
            }
            catch (SQLException e) {
                // 4. 发送快照失败的通知给监控/通知服务
                notificationService.initialSnapshotNotificationService().notifyCompletedTableWithError(snapshotContext.partition,
                        snapshotContext.offset,
                        table.id().identifier());
                // 5. 包装成 ConnectException 抛出，这将导致整个快照任务失败并停止连接器
                throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
            }
            finally {
                offsets.add(offset);
                connectionPool.add(connection);
            }
            return null;
        };
    }
    // Debezium 快照流程中最底层的“体力活”执行者。它负责具体某一张表的数据读取、迭代、转换以及发送。
    protected void doCreateDataEventsForTable(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<P, O> snapshotContext, O offset,
                                              SnapshotReceiver<P> snapshotReceiver, Table table,
                                              boolean firstTable, boolean lastTable, int tableOrder, int tableCount, String selectStatement, OptionalLong rowCount,
                                              Set<TableId> rowCountTablesKeySet, JdbcConnection jdbcConnection)
            throws InterruptedException, SQLException {
        // 1. 检查上下文状态，如果任务被取消或停止，立即抛出中断异常
        if (!sourceContext.isRunning()) {
            throw new InterruptedException("Interrupted while snapshotting table " + table.id());
        }
        // 2. 记录导出开始时间，用于后续计算耗时
        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("Exporting data from table '{}' ({} of {} tables)", table.id(), tableOrder, tableCount);
        // 3. 通过通知服务发布“表快照进行中”的消息，方便外部监控
        notificationService.initialSnapshotNotificationService().notifyTableInProgress(
                snapshotContext.partition,
                snapshotContext.offset,
                table.id().identifier(),
                rowCountTablesKeySet);
        // 4. 获取当前表快照的源时间戳（通常是数据库服务器的当前时间）
        Instant sourceTableSnapshotTimestamp = getSnapshotSourceTimestamp(jdbcConnection, offset, table.id());
        // 5. 使用 try-with-resources 自动管理资源
        // readTableStatement：根据表行数优化 Statement（例如设置合适的 Fetch Size 防止内存溢出）
        try (Statement statement = readTableStatement(jdbcConnection, rowCount);
                // 执行 SELECT 语句获取结果集
                ResultSet rs = resultSetForDataEvents(selectStatement, statement)) {
            // 6. 将结果集的列结构映射为数组索引，提高后续读取每一行数据的效率
            ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
            long rows = 0;
            Timer logTimer = getTableScanLogTimer();
            // 检查是否有数据
            boolean hasNext = rs.next();

            if (hasNext) {
                while (hasNext) {
                    // 7. 循环内部再次检查运行状态，支持实时停止
                    if (!sourceContext.isRunning()) {
                        throw new InterruptedException("Interrupted while snapshotting table " + table.id());
                    }

                    rows++;
                    // 8. 将当前行数据转换为 Object 数组（Debezium 内部通用格式）
                    final Object[] row = jdbcConnection.rowToArray(table, rs, columnArray);
                    // 9. 定时打印进度日志（例如每 10 秒或每 10000 行）
                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        if (rowCount.isPresent()) {
                            LOGGER.info("\t Exported {} of {} records for table '{}' after {}", rows, rowCount.getAsLong(),
                                    table.id(), Strings.duration(stop - exportStart));
                        }
                        else {
                            LOGGER.info("\t Exported {} records for table '{}' after {}", rows, table.id(),
                                    Strings.duration(stop - exportStart));
                        }
                        snapshotProgressListener.rowsScanned(snapshotContext.partition, table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }
                    // 10. 读取下一行并判断是否结束
                    hasNext = rs.next();
                    // 11. 设置快照标记位（标记是否是当前表的最后一行，是否是整个快照任务的最后一条记录）
                    // 这决定了 Kafka 消息中 'snapshot' 字段的值（true/last/false）
                    setSnapshotMarker(offset, firstTable, lastTable, rows == 1, !hasNext);
                    // 12. 分发快照事件
                    // getChangeRecordEmitter 将数组格式转换为 Kafka Connect 的 Struct
                    // dispatchSnapshotEvent 负责将事件传递给后续通道进行序列化和发送
                    dispatcher.dispatchSnapshotEvent(snapshotContext.partition, table.id(),
                            getChangeRecordEmitter(snapshotContext.partition, offset, table.id(), row, sourceTableSnapshotTimestamp), snapshotReceiver);
                }
            }
            else {
                setSnapshotMarker(offset, firstTable, lastTable, false, true);
            }

            LOGGER.info("\t Finished exporting {} records for table '{}' ({} of {} tables); total duration '{}'",
                    rows, table.id(), tableOrder, tableCount, Strings.duration(clock.currentTimeInMillis() - exportStart));
            snapshotProgressListener.dataCollectionSnapshotCompleted(snapshotContext.partition, table.id(), rows);
            // 16. 发送成功完成该表快照的通知
            notificationService.initialSnapshotNotificationService().notifyCompletedTableSuccessfully(snapshotContext.partition,
                    snapshotContext.offset, table.id().identifier(), rows, snapshotContext.capturedTables);
        }
    }

    protected ResultSet resultSetForDataEvents(String selectStatement, Statement statement)
            throws SQLException {
        return CancellableResultSet.from(statement.executeQuery(selectStatement));
    }

    private void setSnapshotMarker(OffsetContext offset, boolean firstTable, boolean lastTable, boolean firstRecordInTable,
                                   boolean lastRecordInTable) {
        if (lastRecordInTable && lastTable) {
            offset.markSnapshotRecord(SnapshotRecord.LAST);
        }
        else if (firstRecordInTable && firstTable) {
            offset.markSnapshotRecord(SnapshotRecord.FIRST);
        }
        else if (lastRecordInTable) {
            offset.markSnapshotRecord(SnapshotRecord.LAST_IN_DATA_COLLECTION);
        }
        else if (firstRecordInTable) {
            offset.markSnapshotRecord(SnapshotRecord.FIRST_IN_DATA_COLLECTION);
        }
        else {
            offset.markSnapshotRecord(SnapshotRecord.TRUE);
        }
    }

    protected void lastSnapshotRecord(RelationalSnapshotContext<P, O> snapshotContext) {
        snapshotContext.offset.markSnapshotRecord(SnapshotRecord.LAST);
    }

    /**
     * If connector is able to provide statistics-based number of records per table.
     */
    protected OptionalLong rowCountForTable(TableId tableId) {
        return OptionalLong.empty();
    }

    private Timer getTableScanLogTimer() {
        return Threads.timer(clock, LOG_INTERVAL);
    }

    /**
     * Returns a {@link ChangeRecordEmitter} producing the change records for the given table row.
     */
    protected ChangeRecordEmitter<P> getChangeRecordEmitter(P partition, O offset, TableId tableId,
                                                            Object[] row, Instant timestamp) {
        offset.event(tableId, timestamp);
        return new SnapshotChangeRecordEmitter<>(partition, offset, row, getClock(), connectorConfig);
    }

    /**
     * Returns a valid query string for the specified table, either given by the user via snapshot select overrides or
     * defaulting to a statement provided by the DB-specific change event source.
     *
     * @param tableId the table to generate a query for
     * @param snapshotSelectOverridesByTable the select overrides by table
     * @return a valid query string or empty if table will not be snapshotted
     */
    private Optional<String> determineSnapshotSelect(RelationalSnapshotContext<P, O> snapshotContext, TableId tableId,
                                                     Map<DataCollectionId, String> snapshotSelectOverridesByTable) {
        if (tableId.equals(signalDataCollectionTableId)) {
            // Skip the signal data collection as data shouldn't be captured
            return Optional.empty();
        }

        String overriddenSelect = getSnapshotSelectOverridesByTable(tableId, snapshotSelectOverridesByTable);
        if (overriddenSelect != null) {
            return Optional.of(enhanceOverriddenSelect(snapshotContext, overriddenSelect, tableId));
        }

        List<String> columns = getPreparedColumnNames(snapshotContext.partition, schema.tableFor(tableId));

        return getSnapshotSelect(snapshotContext, tableId, columns);
    }

    protected String getSnapshotSelectOverridesByTable(TableId tableId, Map<DataCollectionId, String> snapshotSelectOverrides) {
        String overriddenSelect = snapshotSelectOverrides.get(tableId);

        // try without catalog id, as this might or might not be populated based on the given connector
        if (overriddenSelect == null) {
            overriddenSelect = snapshotSelectOverrides.get(new TableId(null, tableId.schema(), tableId.table()));
        }

        return overriddenSelect;
    }

    /**
     * Prepares a list of columns to be used in the snapshot select.
     * The selected columns are based on the column include/exclude filters and if all columns are excluded,
     * the list will contain all the primary key columns.
     *
     * @return list of snapshot select columns
     */
    protected List<String> getPreparedColumnNames(P partition, Table table) {
        List<String> columnNames = table.retrieveColumnNames()
                .stream()
                .filter(columnName -> additionalColumnFilter(partition, table.id(), columnName))
                .filter(columnName -> connectorConfig.getColumnFilter().matches(table.id().catalog(), table.id().schema(), table.id().table(), columnName))
                .map(jdbcConnection::quoteIdentifier)
                .collect(Collectors.toList());

        if (columnNames.isEmpty()) {
            LOGGER.info("\t All columns in table {} were excluded due to include/exclude lists, defaulting to selecting all columns", table.id());

            columnNames = table.retrieveColumnNames()
                    .stream()
                    .map(jdbcConnection::quoteIdentifier)
                    .collect(Collectors.toList());
        }

        return columnNames;
    }

    /**
     * Additional filter handling for preparing column names for snapshot select
     */
    protected boolean additionalColumnFilter(P partition, TableId tableId, String columnName) {
        return true;
    }

    /**
     * This method is overridden for Oracle to implement "as of SCN" predicate
     * @param snapshotContext snapshot context, used for getting offset SCN
     * @param overriddenSelect conditional snapshot select
     * @return enhanced select statement. By default it just returns original select statements.
     */
    protected String enhanceOverriddenSelect(RelationalSnapshotContext<P, O> snapshotContext, String overriddenSelect,
                                             TableId tableId) {
        return overriddenSelect;
    }

    /**
     * Returns the SELECT statement to be used for scanning the given table or empty value if
     * the table will be streamed from but not snapshotted
     */
    // TODO Should it be Statement or similar?
    // TODO Handle override option generically; a problem will be how to handle the dynamic part (Oracle's "... as of
    // scn xyz")
    protected abstract Optional<String> getSnapshotSelect(RelationalSnapshotContext<P, O> snapshotContext,
                                                          TableId tableId, List<String> columns);

    protected Optional<String> getSnapshotConnectionFirstSelect(RelationalSnapshotContext<P, O> snapshotContext, TableId tableId) {
        return Optional.empty();
    }

    /**
     * Allow per-connector query creation to override for best database performance depending on the table size.
     */
    protected Statement readTableStatement(JdbcConnection jdbcConnection, OptionalLong tableSize) throws SQLException {
        return jdbcConnection.readTableStatement(connectorConfig, tableSize);
    }

    private void rollbackTransaction(Connection connection) {
        if (connection != null) {
            try {
                connection.rollback();
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    public static class RelationalSnapshotContext<P extends Partition, O extends OffsetContext>
            extends SnapshotContext<P, O> {
        // 当前快照所属的数据库目录（Catalog）名称
        public final String catalogName;
        // 用于存储从数据库读取到的内存表模型
        public final Tables tables;
        // 标识这是否是一次“按需快照”（通过信号触发的快照）
        public final boolean onDemand;
        // 存储经过过滤器筛选后，本次快照确定要读取数据的表清单
        public Set<TableId> capturedTables;
        // 存储本次快照需要捕获结构信息的表清单。
        public Set<TableId> capturedSchemaTables;

        public RelationalSnapshotContext(P partition, String catalogName, boolean onDemand) {
            super(partition);
            this.catalogName = catalogName;
            this.tables = new Tables();
            this.onDemand = onDemand;
        }
    }

    protected Clock getClock() {
        return clock;
    }

    protected void postSnapshot() throws InterruptedException {
    }

    protected void preSnapshot() throws InterruptedException {

    }
}
