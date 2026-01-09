/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.function.Predicates;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryMetrics;

/**
 * Configuration options shared across the relational CDC connectors which use a persistent database schema history.
 *
 * @author Gunnar Morling
 */
// 专门为那些需要持久化存储数据库结构（Schema）历史的连接器（如 MySQL, SQL Server, Oracle 等）提供通用的配置逻辑。
// 在 CDC（数据变更捕获）过程中，数据库的表结构会随时间变化（DDL 操作）。为了能够正确解析很久以前的 Binlog 或日志记录，Debezium 必须记录结构变化的每一个版本，而这个类就是管理这些“历史记录”配置的核心。
// 定义 Schema 历史配置：提供如何存储（如 Kafka, 文件, 数据库等）和恢复数据库结构历史的标准化参数。
// 管理 DDL 过滤逻辑：决定哪些 DDL 语句（如 CREATE, ALTER）应该被记录，哪些应该被忽略。
// 实例化历史库组件：负责根据配置实例化 SchemaHistory 对象，并为其注入必要的运行参数。

public abstract class HistorizedRelationalDatabaseConnectorConfig extends RelationalDatabaseConnectorConfig {
    // 快照阶段默认每次从数据库拉取的行数（2000行）。
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 2_000;
    // 默认的历史记录存储实现类：KafkaSchemaHistory。
    private static final String DEFAULT_SCHEMA_HISTORY = "io.debezium.storage.kafka.history.KafkaSchemaHistory";
    // 标识在标识符命名中，Catalog 是否位于 Schema 之前（如 MySQL 中 catalog 即 database）。
    private final boolean useCatalogBeforeSchema;
    // 当前连接器的类类型（用于在历史记录中标识来源）。
    private final Class<? extends SourceConnector> connectorClass;
    // 是否处于多分区模式。
    private final boolean multiPartitionMode;
    // 用于过滤不需要记录到历史中的 DDL 语句（基于正则表达式）。
    private final Predicate<String> ddlFilter;
    // 当遇到无法解析的 DDL 语句时，是跳过还是抛出异常中断。
    protected boolean skipUnparseableDDL;
    // 是否只存储那些“被列入捕获名单（Include List）”的表的结构变更。
    protected boolean storeOnlyCapturedTablesDdl;
    // 是否只存储那些“被列入捕获名单”的数据库的结构变更。
    protected boolean storeOnlyCapturedDatabasesDdl;

    /**
     * The database schema history class is hidden in the {@link #configDef()} since that is designed to work with a user interface,
     * and in these situations using Kafka is the only way to go.
     */
    // 定义使用哪个类来存储历史
    public static final Field SCHEMA_HISTORY = Field.create("schema.history.internal")
            .withDisplayName("Database schema history class")
            .withType(Type.CLASS)
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withInvisibleRecommender()
            .withDescription("The name of the SchemaHistory class that should be used to store and recover database schema changes. "
                    + "The configuration properties for the history are prefixed with the '"
                    + SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + "' string.")
            .withDefault(DEFAULT_SCHEMA_HISTORY);
    // 是否跳过解析失败的 DDL。
    public static final Field SKIP_UNPARSEABLE_DDL_STATEMENTS = SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS;
    // 开启后，非捕获表的 ALTER TABLE 等操作不会存入历史，减小历史记录体积。
    public static final Field STORE_ONLY_CAPTURED_TABLES_DDL = SchemaHistory.STORE_ONLY_CAPTURED_TABLES_DDL;

    public static final Field STORE_ONLY_CAPTURED_DATABASES_DDL = SchemaHistory.STORE_ONLY_CAPTURED_DATABASES_DDL;

    protected static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .history(
                    SCHEMA_HISTORY,
                    SKIP_UNPARSEABLE_DDL_STATEMENTS,
                    STORE_ONLY_CAPTURED_TABLES_DDL,
                    STORE_ONLY_CAPTURED_DATABASES_DDL)
            .create();

    protected HistorizedRelationalDatabaseConnectorConfig(Class<? extends SourceConnector> connectorClass,
                                                          Configuration config,
                                                          TableFilter systemTablesFilter,
                                                          boolean useCatalogBeforeSchema,
                                                          int defaultSnapshotFetchSize,
                                                          ColumnFilterMode columnFilterMode,
                                                          boolean multiPartitionMode) {
        this(connectorClass, config, systemTablesFilter, TableId::toString, useCatalogBeforeSchema,
                defaultSnapshotFetchSize, columnFilterMode, multiPartitionMode);
    }

    protected HistorizedRelationalDatabaseConnectorConfig(Class<? extends SourceConnector> connectorClass,
                                                          Configuration config,
                                                          TableFilter systemTablesFilter,
                                                          TableIdToStringMapper tableIdMapper,
                                                          boolean useCatalogBeforeSchema,
                                                          ColumnFilterMode columnFilterMode,
                                                          boolean multiPartitionMode) {
        this(connectorClass, config, systemTablesFilter, tableIdMapper, useCatalogBeforeSchema,
                DEFAULT_SNAPSHOT_FETCH_SIZE, columnFilterMode, multiPartitionMode);
    }

    protected HistorizedRelationalDatabaseConnectorConfig(Class<? extends SourceConnector> connectorClass,
                                                          Configuration config,
                                                          TableFilter systemTablesFilter,
                                                          TableIdToStringMapper tableIdMapper,
                                                          boolean useCatalogBeforeSchema,
                                                          int defaultSnapshotFetchSize,
                                                          ColumnFilterMode columnFilterMode,
                                                          boolean multiPartitionMode) {
        super(config, systemTablesFilter, tableIdMapper, defaultSnapshotFetchSize, columnFilterMode, useCatalogBeforeSchema);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;
        this.connectorClass = connectorClass;
        this.multiPartitionMode = multiPartitionMode;
        this.ddlFilter = createDdlFilter(config);
        this.skipUnparseableDDL = config.getBoolean(SKIP_UNPARSEABLE_DDL_STATEMENTS);
        this.storeOnlyCapturedTablesDdl = config.getBoolean(STORE_ONLY_CAPTURED_TABLES_DDL);
        this.storeOnlyCapturedDatabasesDdl = config.getBoolean(STORE_ONLY_CAPTURED_DATABASES_DDL);
    }

    /**
     * Returns a configured (but not yet started) instance of the database schema history.
     */
    public SchemaHistory getSchemaHistory() {
        Configuration config = getConfig();

        SchemaHistory schemaHistory = config.getInstance(SCHEMA_HISTORY, SchemaHistory.class);
        if (schemaHistory == null) {
            throw new ConnectException("Unable to instantiate the database schema history class " +
                    config.getString(SCHEMA_HISTORY));
        }

        // Do not remove the prefix from the subset of config properties ...
        Configuration schemaHistoryConfig = config.subset(SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING, false)
                .edit()
                .with(config.subset(Field.INTERNAL_PREFIX + SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING, false))
                .withDefault(SchemaHistory.NAME, getLogicalName() + "-schemahistory")
                .withDefault(SchemaHistory.INTERNAL_CONNECTOR_CLASS, connectorClass.getName())
                .withDefault(SchemaHistory.INTERNAL_CONNECTOR_ID, logicalName)
                .build();

        HistoryRecordComparator historyComparator = getHistoryRecordComparator();
        schemaHistory.configure(schemaHistoryConfig, historyComparator,
                new SchemaHistoryMetrics(this, multiPartitionMode()), useCatalogBeforeSchema()); // validates

        return schemaHistory;
    }

    public boolean useCatalogBeforeSchema() {
        return useCatalogBeforeSchema;
    }

    public boolean multiPartitionMode() {
        return multiPartitionMode;
    }

    private Predicate<String> createDdlFilter(Configuration config) {
        // Set up the DDL filter
        final String ddlFilter = config.getString(SchemaHistory.DDL_FILTER);
        return (ddlFilter != null) ? Predicates.includes(ddlFilter, Pattern.CASE_INSENSITIVE | Pattern.DOTALL) : (x -> false);
    }

    public Predicate<String> ddlFilter() {
        return ddlFilter;
    }

    public boolean skipUnparseableDdlStatements() {
        return skipUnparseableDDL;
    }

    public boolean storeOnlyCapturedTables() {
        return storeOnlyCapturedTablesDdl;
    }

    public boolean storeOnlyCapturedDatabases() {
        return storeOnlyCapturedDatabasesDdl;
    }

    /**
     * Returns a comparator to be used when recovering records from the schema history, making sure no history entries
     * newer than the offset we resume from are recovered (which could happen when restarting a connector after history
     * records have been persisted but no new offset has been committed yet).
     */
    public abstract HistoryRecordComparator getHistoryRecordComparator();

}
