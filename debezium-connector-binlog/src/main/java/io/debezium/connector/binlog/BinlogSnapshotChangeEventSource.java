/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection.DatabaseLocales;
import io.debezium.connector.binlog.metrics.BinlogSnapshotChangeEventSourceMetrics;
import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.relational.RelationalDatabaseConnectorConfig.SnapshotTablesRowCountOrder;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
import io.debezium.util.Collect;
import io.debezium.util.Strings;
import io.debezium.util.Threads;

/**
 * An abstract implementation of {@link SnapshotChangeEventSource} for binlog-based connectors.
 *
 * @author Chris Cranford
 */
// Debezium 中专门为**基于 Binlog 的数据库（如 MySQL、MariaDB）**设计的快照（Snapshot）源抽象类。
// 在通用的关系数据库快照逻辑基础上，增加了 Binlog 特有的全局锁管理、一致性快照点定位（GTID/Binlog Position）以及多线程 Schema 采集等功能。
// 该类的主要职责是在 Connector 启动的“全量阶段”执行以下任务：
// 一致性保证：通过全局读锁（FLUSH TABLES WITH READ LOCK）或表级锁，确保快照开始时的数据库状态是静态的。
// 获取偏移量：在持有锁的瞬间，记录当前的 Binlog 文件名、位置（Position）或 GTID，作为后续增量同步的起点。
// Schema 导出：通过 SHOW CREATE TABLE 捕获表结构，重建内存中的表元数据。
// 数据导出：将配置的表数据转换成 SourceRecord 发送到 Kafka。
// 锁心跳管理：为了防止长连接在长时间快照过程中被数据库断开导致锁失效，它提供了一个后台心跳机制来维持连接活跃。

public abstract class BinlogSnapshotChangeEventSource<P extends BinlogPartition, O extends BinlogOffsetContext<?>>
        extends RelationalSnapshotChangeEventSource<P, O> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogSnapshotChangeEventSource.class);
    private static final Logger ROW_ESTIMATE_LOGGER = LoggerFactory.getLogger(BinlogSnapshotChangeEventSource.class.getName() + ".RowEstimate");
    // 锁心跳间隔，硬编码为 30 秒。
    private static final Duration LOCK_HEARTBEAT_INTERVAL = Duration.ofSeconds(30);
    // 连接器的配置信息（如锁超时时间、线程数等）。
    private final BinlogConnectorConfig connectorConfig;
    // 与数据库的主 JDBC 连接。
    private final BinlogConnectorConnection connection;
    // 过滤器，决定哪些数据库和表需要被包含或排除。
    private final RelationalTableFilters filters;
    // 记录快照进度、锁状态等监控指标。
    private final BinlogSnapshotChangeEventSourceMetrics<P> metrics;
    // 维护内存中的表结构信息。
    private final BinlogDatabaseSchema<P, O, ?, ?> databaseSchema;
    // 存储快照期间生成的模式变更事件（如 CREATE TABLE）。
    private final Set<SchemaChangeEvent> schemaEvents = new LinkedHashSet<>();
    // 处理快照结束前的最后一个事件的处理器。
    private final BlockingConsumer<Function<SourceRecord, SourceRecord>> lastEventProcessor;
    // 快照开始前的自定义钩子动作。
    private final Runnable preSnapshotAction;
    // 需要延迟捕获 Schema 的表集合（用于两阶段快照模式）。
    private Set<TableId> delayedSchemaSnapshotTables = Collections.emptySet();
    // 记录获取全局读锁的时间戳，-1 表示未加锁。
    private long globalLockAcquiredAt = -1;
    // 记录获取表级锁的时间戳。
    private long tableLockAcquiredAt = -1;
    // 调度器，负责定期执行心跳 SQL 以维持锁连接。
    private ScheduledExecutorService lockKeepAliveExecutor;
    /**
     * Guard object to serialize access to the not-thread-safe {@link #connection} between the
     * snapshot thread and the keep-alive heartbeat task.
     */
    // 互斥锁，防止心跳线程和主快照线程同时使用非线程安全的 JDBC 连接。
    private final Object binlogConnectionMutex = new Object();

    public BinlogSnapshotChangeEventSource(BinlogConnectorConfig connectorConfig,
                                           MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory,
                                           BinlogDatabaseSchema<P, O, ?, ?> schema,
                                           EventDispatcher<P, TableId> dispatcher,
                                           Clock clock,
                                           BinlogSnapshotChangeEventSourceMetrics<P> metrics,
                                           BlockingConsumer<Function<SourceRecord, SourceRecord>> lastEventProcessor,
                                           Runnable preSnapshotAction,
                                           NotificationService<P, O> notificationService,
                                           SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, metrics, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
        this.connection = connectionFactory.mainConnection();
        this.filters = connectorConfig.getTableFilters();
        this.metrics = metrics;
        this.databaseSchema = schema;
        this.lastEventProcessor = lastEventProcessor;
        this.preSnapshotAction = preSnapshotAction;
    }
    // 在 Debezium 的快照工作流中起到了**初始化上下文（Context）**的关键作用。
    // 参数 P partition: 代表当前处理的分区信息（例如 MySQL 的主机名和端口）。
    // 参数 boolean onDemand: 标识这是一个“增量/按需快照”（信号触发）还是“初始快照”（启动触发）。
    @Override
    protected SnapshotContext<P, O> prepare(P partition, boolean onDemand) {
        return new BinlogSnapshotContext<>(partition, onDemand);
    }
    // 快照流程中的元数据扫描阶段。
    // 它的核心任务是从数据库中拉取所有可用的表清单，并初步验证过滤器的生效情况。
    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<P, O> ctx) throws Exception {
        // 1. 从数据库连接中获取所有表的原始清单
        Set<TableId> allTableIds = connection.getAllTableIds(ctx.catalogName);
        // Log the databases that were readable and are included based on filters
        // 2. 利用 Java Stream 对获取到的所有表进行过滤分析，提取出用户关心的数据库名称
        final Set<String> includedDatabaseNames = allTableIds.stream()
                .map(TableId::catalog)
                .filter(filters.databaseFilter())
                .collect(Collectors.toSet());
        LOGGER.info("\tsnapshot continuing with database(s): {}", includedDatabaseNames);

        return allTableIds;
    }
    // Debezium 快照流程中确保**一致性（Consistency）**的关键一步。
    // 它的核心目标是：在获取数据库结构（Schema）和当前的 Binlog 位点时，通过锁机制让数据库在这一瞬间“静止”。
    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<P, O> snapshotContext)
            throws SQLException {
        // Set the transaction isolation level to REPEATABLE READ. This is the default, but the default can be changed
        // which is why we explicitly set it here.
        //
        // With REPEATABLE READ, all SELECT queries within the scope of a transaction (which we don't yet have) will read
        // from the same MVCC snapshot. Thus each plain (non-locking) SELECT statements within the same transaction are
        // consistent also with respect to each other.
        //
        // See: https://dev.mysql.com/doc/refman/8.2/en/set-transaction.html
        // See: https://dev.mysql.com/doc/refman/8.2/en/innodb-transaction-isolation-levels.html
        // See: https://dev.mysql.com/doc/refman/8.2/en/innodb-consistent-read.html
        // 1. 设置 JDBC 事务隔离级别为 REPEATABLE READ (可重复读)
        // 这是实现一致性快照的前提，确保在同一个事务内多次读取的数据是一致的。
        connection.connection().setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        // 2. 设置普通锁等待超时时间
        // 如果无法立即获得锁，连接器最多等待配置的时间，避免无限期阻塞
        connection.executeWithoutCommitting("SET SESSION lock_wait_timeout=" + connectorConfig.snapshotLockTimeout().getSeconds());
        try {
            // 3. 尝试设置 InnoDB 引擎特有的锁等待超时时间
            connection.executeWithoutCommitting("SET SESSION innodb_lock_wait_timeout=" + connectorConfig.snapshotLockTimeout().getSeconds());
        }
        catch (SQLException e) {
            LOGGER.warn("Unable to set innodb_lock_wait_timeout", e);
        }

        // ------------------------------------
        // LOCK TABLES
        // ------------------------------------
        // Obtain read lock on all tables. This statement closes all open tables and locks all tables
        // for all databases with a global read lock, and it prevents ALL updates while we have this lock.
        // It also ensures that everything we do while we have this lock will be consistent.
        // 检查配置：locking.strategy 是否允许锁，以及是否请求了全局锁 (snapshot.locking.mode)。
        if (connectorConfig.getSnapshotLockingStrategy().isLockingEnabled() && connectorConfig.isGlobalLockUseRequested()) {
            try {
                // 5. 执行全局读锁 (通常是执行 FLUSH TABLES WITH READ LOCK)
                // 这会让整个数据库处于只读状态，防止任何 DDL 或 DML 操作。
                globalLock();
                metrics.setGlobalLockAcquired();
            }
            catch (SQLException e) {
                LOGGER.info("Unable to flush and acquire global read lock, will use table read locks after reading table names");
                // Continue anyway, since RDS (among others) don't allow setting a global lock
                assert !isGloballyLocked();
            }
            // 7. 检查是否需要在 FLUSH 后重置隔离级别
            // 在某些 MySQL 版本中，FLUSH TABLES 操作可能会隐式提交事务或重置会话变量。
            if (connectorConfig.getSnapshotLockingStrategy().isIsolationLevelResetOnFlush()) {
                // FLUSH TABLES resets TX and isolation level
                connection.executeWithoutCommitting("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            }

            if (connectorConfig.getSnapshotLockingStrategy().useConsistentSnapshotTransaction()) {
                try {
                    connection.executeWithoutCommitting("START TRANSACTION WITH CONSISTENT SNAPSHOT");
                }
                catch (SQLException e) {
                    LOGGER.error("It's possible to receive duplicated events between snapshot and streaming. It can be caused by an unsupported engine");

                    throw e;
                }
            }
        }
    }
    // releaseSchemaSnapshotLocks 方法的作用是在完成 Schema（表结构） 捕获和 Binlog 位点 确定后，尝试释放之前加上的锁。
    // 其核心逻辑在于平衡“尽早释放锁以恢复数据库写能力”与“保证数据一致性”之间的矛盾。
    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<P, O> snapshotContext) throws SQLException {
        if (connectorConfig.getSnapshotLockingStrategy().isMinimalLockingEnabled()) {
            if (isGloballyLocked()) {
                globalUnlock();
            }
            if (isTablesLocked()) {
                // We could not acquire a global read lock and instead had to obtain individual table-level read locks
                // using 'FLUSH TABLE <tableName> WITH READ LOCK'. However, if we were to do this, the 'UNLOCK TABLES'
                // would implicitly commit our active transaction, and this would break our consistent snapshot logic.
                // Therefore, we cannot unlock the tables here!
                // https://dev.mysql.com/doc/refman/8.2/en/flush.html
                LOGGER.warn("Tables were locked explicitly, but to get a consistent snapshot we cannot release the locks until we've read all tables.");
            }
        }
    }
    // releaseDataSnapshotLocks 是快照流程中数据读取阶段结束后的清理方法。
    // 它的职责是彻底释放数据库锁，并处理在“两阶段快照”模式下遗留的表结构（Schema）捕获任务。
    @Override
    protected void releaseDataSnapshotLocks(RelationalSnapshotContext<P, O> snapshotContext) throws Exception {
        // 1. 如果当前持有全局锁，则释放。
        // 在初始快照完全结束后，必须确保全局锁被释放，以恢复数据库的正常写入。
        if (isGloballyLocked()) {
            globalUnlock();
        }
        // 2. 如果持有的是表级锁，则释放。
        if (isTablesLocked()) {
            tableUnlock();
            // 当使用表锁且开启了特殊配置时，Debezium 可能只锁定了部分表（需要读数据的表），
            // 而将其他表的 Schema 捕获推迟到了现在。
            if (!delayedSchemaSnapshotTables.isEmpty()) {
                // 清空当前的模式事件缓存，准备重新填充
                schemaEvents.clear();
                if (connectorConfig.getSnapshotLockingStrategy().isLockingEnabled()) {
                    // 如果启用了锁，直接在当前线程同步获取这些表的 DDL
                    createSchemaEventsForTables(snapshotContext, delayedSchemaSnapshotTables, false);
                }
                else {
                    // 如果未启用锁，为了提高效率，开启多线程并行获取 DDL
                    int snapshotMaxThreads = connectionPool.size();
                    LOGGER.info("Creating delayed schema snapshot worker pool with {} worker thread(s)", snapshotMaxThreads);
                    ExecutorService executorService = Executors.newFixedThreadPool(snapshotMaxThreads);
                    try {
                        createSchemaEventsForTables(snapshotContext, delayedSchemaSnapshotTables, false, executorService);
                    }
                    finally {
                        // 任务完成后关闭线程池
                        executorService.shutdownNow();
                    }
                }
                // 获取到 DDL 后，需要将这些变更通知给 Debezium 的其他组件（如 Schema History）。
                for (final SchemaChangeEvent event : schemaEvents) {
                    // 过滤器检查：如果配置为只存储被捕获表的 DDL，则过滤掉不属于目标数据库的事件
                    if (databaseSchema.storeOnlyCapturedTables()
                            && event.getDatabase() != null
                            && !event.getDatabase().isEmpty()
                            && !connectorConfig.getTableFilters().databaseFilter().test(event.getDatabase())) {
                        LOGGER.debug("Skipping schema event as it belongs to a non-captured database: '{}'", event);
                        continue;
                    }

                    LOGGER.debug("Processing schema event {}", event);
                    // 6. 确定事件关联的表 ID，并更新偏移量上下文的时间戳
                    final TableId tableId = event.getTables().isEmpty() ? null : event.getTables().iterator().next().id();
                    snapshotContext.offset.event(tableId, getClock().currentTime());
                    // 7. 分发 Schema 变更事件
                    // 这步非常关键，它会将 DDL 发送到 Kafka 的 Schema History Topic 中
                    dispatcher.dispatchSchemaChangeEvent(snapshotContext.partition, snapshotContext.offset, tableId, (receiver) -> receiver.schemaChangeEvent(event));
                }

                // Make schema available for snapshot source
                // 8. 刷新内存模式映射
                databaseSchema.tableIds().forEach(x -> snapshotContext.tables.overwriteTable(databaseSchema.tableFor(x)));
            }
        }
    }
    // 核心作用是：在快照开始前，确定数据库当前的日志位置（Binlog Position 或 GTID），作为快照结束后增量读取（Streaming）的起点。
    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<P, O> ctx, O previousOffset) throws Exception {
        // 1. 锁状态安全检查
        // 如果配置要求加锁，但当前既没有全局锁也没有表锁，则直接返回。
        // 这是一种保护机制，确保在没有获得一致性视图的情况下不进行位点计算，防止数据空隙。
        if (!isGloballyLocked() && !isTablesLocked() && connectorConfig.getSnapshotLockingStrategy().isLockingEnabled()) {
            return;
        }
        // 2. 检查是否需要跳过位点获取（增量快照或特定快照模式场景）
        // 如果存在之前的位点（previousOffset），且当前的 Snapshotter（快照控制器）认为
        // 不需要从快照位置开始流式传输（例如：恢复之前的快照任务），则复用旧位点。
        if (previousOffset != null && !snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot()) {
            ctx.offset = previousOffset;
            tryStartingSnapshot(ctx);
            return;
        }
        // 3. 初始化新的偏移量上下文 (Offset Context)
        // 根据连接器配置（如服务器 ID、集群名等）创建一个空的位点对象。
        final O offsetContext = getInitialOffsetContext(connectorConfig);
        ctx.offset = offsetContext;
        // 4. 获取数据库当前的实时位点 (核心步骤)
        // 这一步会通过 connection 执行 "SHOW MASTER STATUS" 或查询 GTID 集合。
        // 由于此方法通常在加锁期间（lockTablesForSchemaSnapshot 之后）调用，
        // 获取到的 Binlog 文件名和 Position 正好对应于当前内存中数据库结构的时间点。
        setOffsetContextBinlogPositionAndGtidDetailsForSnapshot(offsetContext, connection, snapshotterService);
        tryStartingSnapshot(ctx);
    }

    protected abstract O getInitialOffsetContext(BinlogConnectorConfig connectorConfig);

    protected abstract void setOffsetContextBinlogPositionAndGtidDetailsForSnapshot(O offsetContext,
                                                                                    BinlogConnectorConnection connection,
                                                                                    SnapshotterService snapshotterService)
            throws Exception;

    private void addSchemaEvent(RelationalSnapshotContext<P, O> snapshotContext, String database, String ddl) {
        List<SchemaChangeEvent> schemaChangeEvents = databaseSchema.parseSnapshotDdl(snapshotContext.partition, ddl, database,
                snapshotContext.offset, clock.currentTimeAsInstant());
        schemaEvents.addAll(new LinkedHashSet<>(schemaChangeEvents));
    }
    // Debezium 快照流程中负责**获取数据库结构（DDL）**的核心环节。
    // 它不仅读取当前的表定义，还负责生成一组“虚拟 DDL 序列”，用于在 Schema History 中重构数据库状态。
    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<P, O> snapshotContext,
                                      O offsetContext,
                                      SnapshottingTask snapshottingTask)
            throws Exception {
        Set<TableId> capturedSchemaTables;
        if (twoPhaseSchemaSnapshot()) {
            // Capture schema of captured tables after they are locked
            // 1. 如果是两阶段模式（通常针对表级锁），先锁定需要读取数据的表
            tableLock(snapshotContext);
            // 2. 在锁的保护下确定 Binlog 位点
            determineSnapshotOffset(snapshotContext, offsetContext);
            // 3. 此时只捕获那些“需要读数据”的表的结构
            capturedSchemaTables = snapshotContext.capturedTables;
            LOGGER.info("Table level locking is in place, the schema will be capture in two phases, now capturing: {}", capturedSchemaTables);
            // 4. 计算出哪些表需要延迟到“解锁后”再捕获结构（即在 history 中需要，但本次快照不读数据的表）
            delayedSchemaSnapshotTables = Collect.minus(snapshotContext.capturedSchemaTables, snapshotContext.capturedTables);
            LOGGER.info("Tables for delayed schema capture: {}", delayedSchemaSnapshotTables);
        }
        // 5. 根据配置决定是捕获“所有表”还是“仅捕获名单中的表”的结构
        if (databaseSchema.storeOnlyCapturedTables()) {
            capturedSchemaTables = snapshotContext.capturedTables;
            LOGGER.info("Only captured tables schema should be captured, capturing: {}", capturedSchemaTables);
        }
        else {
            capturedSchemaTables = snapshotContext.capturedSchemaTables;
            LOGGER.info("All eligible tables schema should be captured, capturing: {}", capturedSchemaTables);
        }
        // 6. 按数据库（Catalog）对表进行分组，方便后续按库处理
        final Map<String, List<TableId>> tablesToRead = capturedSchemaTables.stream()
                .collect(Collectors.groupingBy(TableId::catalog, LinkedHashMap::new, Collectors.toList()));
        final Set<String> databases = tablesToRead.keySet();
        // 7. 如果不是按需快照（即初始快照），记录系统字符集变量
        // 这确保了 Schema History 恢复时，字符集环境与源库一致
        if (!snapshottingTask.isOnDemand()) {
            // Record default charset
            addSchemaEvent(snapshotContext, "", connection.setStatementFor(connection.readCharsetSystemVariables()));
        }
        // 8. 为每个表生成虚拟的 DROP TABLE 语句
        // 这样做是为了保证 Schema History 在重播时，如果已存在同名表，能干净地替换掉
        for (TableId tableId : capturedSchemaTables) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while emitting initial DROP TABLE events");
            }
            addSchemaEvent(snapshotContext, tableId.catalog(), "DROP TABLE IF EXISTS " + connection.quotedTableIdString(tableId));
        }
        // 9. 读取所有数据库的排序规则（Collation）和字符集
        final Map<String, DatabaseLocales> databaseCharsets = connection.readDatabaseCollations();

        ExecutorService executorService = null;
        // 10. 如果未启用锁策略，则创建线程池以并行读取各个表的结构（提高效率）
        if (!connectorConfig.getSnapshotLockingStrategy().isLockingEnabled()) {
            int snapshotMaxThreads = connectionPool.size();
            LOGGER.info("Creating schema snapshot worker pool with {} worker thread(s)", snapshotMaxThreads);
            executorService = Executors.newFixedThreadPool(snapshotMaxThreads);
        }
        try {
            // 循环处理每个数据库及其表
            for (String database : databases) {
                if (!sourceContext.isRunning()) {
                    throw new InterruptedException("Interrupted while reading structure of schema " + databases);
                }
                // 11. 对于初始快照，生成创建数据库的虚拟 DDL (DROP + CREATE + USE)
                if (!snapshottingTask.isOnDemand()) {
                    // in case of blocking snapshot we want to read structures only for collections specified in the signal
                    LOGGER.info("Reading structure of database '{}'", database);
                    addSchemaEvent(snapshotContext, database, "DROP DATABASE IF EXISTS " + connection.quoteIdentifier(database));
                    final StringBuilder createDatabaseDdl = new StringBuilder("CREATE DATABASE " + connection.quoteIdentifier(database));
                    final DatabaseLocales defaultDatabaseLocales = databaseCharsets.get(database);
                    if (defaultDatabaseLocales != null) {
                        defaultDatabaseLocales.appendToDdlStatement(database, createDatabaseDdl);
                    }
                    addSchemaEvent(snapshotContext, database, createDatabaseDdl.toString());
                    addSchemaEvent(snapshotContext, database, "USE " + connection.quoteIdentifier(database));
                }
                // 12. 核心操作：为数据库中的每张表生成 SHOW CREATE TABLE 语句并解析
                // 如果有锁，同步执行；无锁，则提交给线程池异步执行
                if (connectorConfig.getSnapshotLockingStrategy().isLockingEnabled()) {
                    createSchemaEventsForTables(snapshotContext, tablesToRead.get(database), true);
                }
                else {
                    assert executorService != null;
                    createSchemaEventsForTables(snapshotContext, tablesToRead.get(database), true, executorService);
                }
            }
        }
        finally {
            if (executorService != null) {
                executorService.shutdownNow();
            }
        }
    }
    // 获取表结构定义（DDL）的“落地”实现。
    // 它的核心任务是通过执行 SQL 命令从数据库中提取真实的建表语句，并将其存入快照上下文中
    private void createSchemaEventsForTables(RelationalSnapshotContext<P, O> snapshotContext,
                                             Collection<TableId> tablesToRead,
                                             boolean firstPhase)
            throws Exception {
        List<TableId> realTablesToRead = new ArrayList<>(tablesToRead);
        if (firstPhase) {
            realTablesToRead = realTablesToRead.stream()
                    .filter(id -> !delayedSchemaSnapshotTables.contains(id))
                    .collect(Collectors.toList());
        }
        for (TableId tableId : realTablesToRead) {
            connection.query("SHOW CREATE TABLE " + connection.quotedTableIdString(tableId), rs -> {
                if (rs.next()) {
                    addSchemaEvent(snapshotContext, tableId.catalog(), rs.getString(2));
                }
            });
        }
    }

    private void createSchemaEventsForTables(RelationalSnapshotContext<P, O> snapshotContext,
                                             Collection<TableId> tablesToRead,
                                             boolean firstPhase,
                                             ExecutorService executorService)
            throws Exception {
        List<TableId> realTablesToRead = new ArrayList<>(tablesToRead);
        if (firstPhase) {
            realTablesToRead = realTablesToRead.stream()
                    .filter(id -> !delayedSchemaSnapshotTables.contains(id))
                    .collect(Collectors.toList());
        }
        if (!realTablesToRead.isEmpty()) {
            CompletionService<Map<TableId, String>> completionService = new ExecutorCompletionService<>(executorService);
            for (TableId tableId : realTablesToRead) {
                completionService.submit(createDdlForTableCallable(tableId, connectionPool));
            }
            Map<TableId, String> ddls = new HashMap<>();
            for (int i = 0; i < realTablesToRead.size(); i++) {
                Map<TableId, String> ddl = completionService.take().get();
                if (ddl != null) {
                    ddls.putAll(ddl);
                }
            }
            ddls.forEach((key, value) -> addSchemaEvent(snapshotContext, key.catalog(), value));
        }
    }

    private Callable<Map<TableId, String>> createDdlForTableCallable(TableId tableId, Queue<JdbcConnection> connectionPool) {
        return () -> {
            JdbcConnection connection = connectionPool.poll();
            assert connection != null;
            try {
                Map<TableId, String> result = new HashMap<>();
                connection.query("SHOW CREATE TABLE " + connection.quotedTableIdString(tableId), rs -> {
                    if (rs.next()) {
                        result.put(tableId, rs.getString(2));
                    }
                });
                return result;
            }
            finally {
                connectionPool.add(connection);
            }
        };
    }

    private boolean twoPhaseSchemaSnapshot() {
        if (!isGloballyLocked() && connectorConfig.getSnapshotLockingStrategy().preventsTableLocks()) {
            // Prevent obtaining individual table-level read locks
            // using 'FLUSH TABLE <tableName> WITH READ LOCK'
            // when using *_no_table_locks mode
            throw new DebeziumException(
                    "Cannot perform two-phase schema snapshot because global read lock was not acquired and table locks are not allowed in *_no_table_locks mode.");
        }
        return connectorConfig.getSnapshotLockingStrategy().isLockingEnabled() && !isGloballyLocked();
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<P, O> snapshotContext,
                                                    Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(
                snapshotContext.partition,
                snapshotContext.offset,
                snapshotContext.catalogName,
                table);
    }

    /**
     * Generate a valid MySQL query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<P, O> snapshotContext,
                                                 TableId tableId,
                                                 List<String> columns) {
        return getSnapshotSelect(tableId, columns);
    }

    private Optional<String> getSnapshotSelect(TableId tableId, List<String> columns) {
        return snapshotterService.getSnapshotQuery().snapshotQuery(tableId.toQuotedString('`'), columns);
    }

    @Override
    protected Optional<String> getSnapshotConnectionFirstSelect(RelationalSnapshotContext<P, O> snapshotContext, TableId tableId) {
        if (getSnapshotSelect(tableId, List.of("*")).isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(getSnapshotSelect(tableId, List.of("*")).get() + " LIMIT 1");
    }

    private boolean isGloballyLocked() {
        return globalLockAcquiredAt != -1;
    }

    private boolean isTablesLocked() {
        return tableLockAcquiredAt != -1;
    }

    private void globalLock() throws SQLException {
        LOGGER.info("Flush and obtain global read lock to prevent writes to database");
        Optional<String> lockingStatement = snapshotterService.getSnapshotLock().tableLockingStatement(null, null);
        if (lockingStatement.isPresent()) {
            connection.executeWithoutCommitting(lockingStatement.get());
            globalLockAcquiredAt = clock.currentTimeInMillis();
            startLockHeartbeat();
        }
    }
    // 释放锁
    private void globalUnlock() throws SQLException {
        // Stop the keep-alive first so that no other thread uses the connection while we release the lock.
        stopLockHeartbeat();
        synchronized (binlogConnectionMutex) {
            LOGGER.info("Releasing global read lock to enable MySQL writes");
            connection.executeWithoutCommitting("UNLOCK TABLES");
        }
        long lockReleased = clock.currentTimeInMillis();
        metrics.setGlobalLockReleased();
        LOGGER.info("Writes to MySQL tables prevented for a total of {}", Strings.duration(lockReleased - globalLockAcquiredAt));
        globalLockAcquiredAt = -1;
        stopLockHeartbeat();
    }

    private void tableLock(RelationalSnapshotContext<P, O> snapshotContext)
            throws SQLException {
        // ------------------------------------
        // LOCK TABLES and READ BINLOG POSITION
        // ------------------------------------
        // We were not able to acquire the global read lock, so instead we have to obtain a read lock on each table.
        // This requires different privileges than normal, and also means we can't unlock the tables without
        // implicitly committing our transaction ...
        if (!connection.userHasPrivileges("LOCK TABLES")) {
            // We don't have the right privileges
            throw new DebeziumException("User does not have the 'LOCK TABLES' privilege required to obtain a "
                    + "consistent snapshot by preventing concurrent writes to tables.");
        }
        // We have the required privileges, so try to lock all of the tables we're interested in ...
        LOGGER.info("Flush and obtain read lock for {} tables (preventing writes)", snapshotContext.capturedTables);
        if (!snapshotContext.capturedTables.isEmpty()) {
            final String tableList = snapshotContext.capturedTables.stream()
                    .map(connection::quotedTableIdString)
                    .collect(Collectors.joining(","));
            connection.executeWithoutCommitting("FLUSH TABLES " + tableList + " WITH READ LOCK");
        }
        tableLockAcquiredAt = clock.currentTimeInMillis();
        metrics.setGlobalLockAcquired();
        startLockHeartbeat();
    }

    private void tableUnlock() throws SQLException {
        // Stop keep-alive before unlocking tables.
        stopLockHeartbeat();
        synchronized (binlogConnectionMutex) {
            LOGGER.info("Releasing table read lock to enable MySQL writes");
            connection.executeWithoutCommitting("UNLOCK TABLES");
        }
        long lockReleased = clock.currentTimeInMillis();
        metrics.setGlobalLockReleased();
        LOGGER.info("Writes to MySQL tables prevented for a total of {}", Strings.duration(lockReleased - tableLockAcquiredAt));
        tableLockAcquiredAt = -1;
        stopLockHeartbeat();
    }

    @Override
    protected OptionalLong rowCountForTable(TableId tableId) {
        if (getSnapshotSelectOverridesByTable(tableId, connectorConfig.getSnapshotSelectOverridesByTable()) != null) {
            return super.rowCountForTable(tableId);
        }
        if (ROW_ESTIMATE_LOGGER.isInfoEnabled() || connectorConfig.snapshotOrderByRowCount() != SnapshotTablesRowCountOrder.DISABLED) {
            OptionalLong rowCount = connection.getEstimatedTableSize(tableId);
            LOGGER.info("Estimated row count for table {} is {}", tableId, rowCount);
            return rowCount;
        }
        return OptionalLong.empty();
    }

    @Override
    protected Statement readTableStatement(JdbcConnection jdbcConnection, OptionalLong rowCount) throws SQLException {
        BinlogConnectorConnection connection = (BinlogConnectorConnection) jdbcConnection;
        final long largeTableRowCount = connectorConfig.getRowCountForLargeTable();
        if (rowCount.isEmpty() || largeTableRowCount == 0 || rowCount.getAsLong() <= largeTableRowCount) {
            return super.readTableStatement(connection, rowCount);
        }
        return createStatementWithLargeResultSet(connection);
    }

    /**
     * Create a JDBC statement that can be used for large result sets.
     * <p>
     * By default, the MySQL Connector/J driver retrieves all rows for ResultSets and stores them in memory. In most cases this
     * is the most efficient way to operate and, due to the design of the MySQL network protocol, is easier to implement.
     * However, when ResultSets that have a large number of rows or large values, the driver may not be able to allocate
     * heap space in the JVM and may result in an {@link OutOfMemoryError}. See
     * <a href="https://issues.jboss.org/browse/DBZ-94">DBZ-94</a> for details.
     * <p>
     * This method handles such cases using the
     * <a href="https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-implementation-notes.html">recommended
     * technique</a> for MySQL by creating the JDBC {@link Statement} with {@link ResultSet#TYPE_FORWARD_ONLY forward-only} cursor
     * and {@link ResultSet#CONCUR_READ_ONLY read-only concurrency} flags, and with a {@link Integer#MIN_VALUE minimum value}
     * {@link Statement#setFetchSize(int) fetch size hint}.
     *
     * @return the statement; never null
     * @throws SQLException if there is a problem creating the statement
     */
    private Statement createStatementWithLargeResultSet(BinlogConnectorConnection connection) throws SQLException {
        int fetchSize = connectorConfig.getSnapshotFetchSize();
        Statement stmt = connection.connection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchSize(fetchSize);
        return stmt;
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class BinlogSnapshotContext<P extends BinlogPartition, O extends BinlogOffsetContext>
            extends RelationalSnapshotContext<P, O> {
        BinlogSnapshotContext(P partition, boolean onDemand) {
            super(partition, "", onDemand);
        }
    }
    // 遍历这些事件，经过过滤后，将它们正式分发给 Schema History 存储并更新内部表模型。
    @Override
    protected void createSchemaChangeEventsForTables(ChangeEventSourceContext sourceContext,
                                                     RelationalSnapshotContext<P, O> snapshotContext,
                                                     SnapshottingTask snapshottingTask)
            throws Exception {
        // 1. 尝试标记快照开始
        // 更新快照上下文的状态，如果快照尚未开始，则记录开始时间戳。
        tryStartingSnapshot(snapshotContext);
        // 2. 遍历在之前步骤中收集到的所有 DDL 事件（如 CREATE DATABASE, CREATE TABLE 等）
        for (final SchemaChangeEvent event : schemaEvents) {
            // 3. 运行状态检查
            // 如果外部任务（如 Kafka Connect）已停止，则抛出异常中断处理。
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while processing event " + event);
            }
            // 4. 根据数据库 Schema 配置进行过滤
            // 检查该事件是否应该被跳过（例如根据配置，某些不关心的 DDL 操作）。
            if (databaseSchema.skipSchemaChangeEvent(event)) {
                continue;
            }

            LOGGER.debug("Processing schema event {}", event);
            // 5. 提取事件关联的 TableId
            // 如果事件关联了表（如 CREATE TABLE），取第一个表的 ID；否则为 null（如 CREATE DATABASE）。
            final TableId tableId = event.getTables().isEmpty() ? null : event.getTables().iterator().next().id();
            if (snapshottingTask.isOnDemand() && !snapshotContext.capturedTables.contains(tableId)) {
                LOGGER.debug("Event {} will be skipped since it's not related to blocking snapshot captured table {}", event, snapshotContext.capturedTables);
                continue;
            }
            snapshotContext.offset.event(tableId, getClock().currentTime());
            // 8. 核心步骤：分发 Schema 变更事件
            // 通过 dispatcher 将 DDL 发送到：
            //   a. 数据库 Schema History (通常是 Kafka 的历史主题)
            //   b. 下游 Schema Change Topic (如果启用了相关配置)
            dispatcher.dispatchSchemaChangeEvent(snapshotContext.partition, snapshotContext.offset, tableId, (receiver) -> receiver.schemaChangeEvent(event));
        }

        // Make schema available for snapshot source
        databaseSchema.tableIds().forEach(x -> snapshotContext.tables.overwriteTable(databaseSchema.tableFor(x)));
    }

    @Override
    protected void postSnapshot() throws InterruptedException {
        // We cannot be sure that the last event as the last one
        // - last table could be empty
        // - data snapshot was not executed
        // - the last table schema snaphsotted is not monitored and storing of monitored is disabled
        lastEventProcessor.accept(record -> {
            record.sourceOffset().remove(BinlogSourceInfo.SNAPSHOT_KEY);
            ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE).put(
                    BinlogSourceInfo.SNAPSHOT_KEY,
                    SnapshotRecord.LAST.toString().toLowerCase());
            return record;
        });
        super.postSnapshot();
    }

    @Override
    protected void preSnapshot() throws InterruptedException {
        preSnapshotAction.run();
        super.preSnapshot();
    }

    @Override
    protected void aborted(SnapshotContext<P, O> snapshotContext) throws InterruptedException {

        lastEventProcessor.accept(Function.identity());

        super.aborted(snapshotContext);
    }
    // 维持快照期间数据库连接的活跃状态
    private void startLockHeartbeat() {
        if (lockKeepAliveExecutor == null || lockKeepAliveExecutor.isShutdown()) {
            LOGGER.info("Starting lock heartbeat");
            lockKeepAliveExecutor = Threads.newSingleThreadScheduledExecutor(
                    getClass(),
                    connectorConfig.getLogicalName(),
                    "lock-heartbeat",
                    true);
            Runnable task = () -> {
                synchronized (binlogConnectionMutex) {
                    try {
                        connection.query("SELECT 1", rs -> {
                        });
                    }
                    catch (SQLException e) {
                        LOGGER.warn("Snapshot lock heartbeat query failed", e);
                    }
                }
            };
            lockKeepAliveExecutor.scheduleAtFixedRate(task, LOCK_HEARTBEAT_INTERVAL.toSeconds(), LOCK_HEARTBEAT_INTERVAL.toSeconds(), TimeUnit.SECONDS);
        }
    }

    private void stopLockHeartbeat() {
        if (lockKeepAliveExecutor != null) {
            lockKeepAliveExecutor.shutdownNow();
            try {
                // Wait briefly to ensure any running task has completed/cancelled.
                lockKeepAliveExecutor.awaitTermination(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            lockKeepAliveExecutor = null;
        }
    }
}
