/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogSnapshotChangeEventSource;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.connector.mysql.MySqlOffsetContext.Loader;
import io.debezium.function.BlockingConsumer;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;
// MySqlSnapshotChangeEventSource 是 Debezium MySQL 连接器中的核心类之一。
// 它继承自 BinlogSnapshotChangeEventSource（这是 Debezium 2.x 以后为了统一 MySQL 和 MariaDB 等 Binlog 协议数据库而引入的基类）。
// 主要职责是执行 MySQL 的全量快照（Snapshot）。当一个 CDC 连接器第一次启动，或者手动触发增量快照时，该类负责：
// 确定一致性读取点：在获取数据库结构和数据之前，获取当前的 Binlog 文件名、位置以及 GTID 集合。
// 数据读取：从配置的表中读取所有现有数据。
// 衔接增量读取：将快照结束时的位点信息传递给 BinlogStreamingChangeEventSource，以便从准确的位置开始消费 Binlog 变更。
public class MySqlSnapshotChangeEventSource extends BinlogSnapshotChangeEventSource<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlSnapshotChangeEventSource.class);
    // 存储 MySQL 特有的配置信息（如是否启用 GTID、数据库连接参数等），在初始化位点上下文时被调用。
    private final MySqlConnectorConfig connectorConfig;

    public MySqlSnapshotChangeEventSource(MySqlConnectorConfig connectorConfig,
                                          MainConnectionProvidingConnectionFactory<BinlogConnectorConnection> connectionFactory,
                                          MySqlDatabaseSchema schema,
                                          EventDispatcher<MySqlPartition, TableId> dispatcher,
                                          Clock clock,
                                          MySqlSnapshotChangeEventSourceMetrics metrics,
                                          BlockingConsumer<Function<SourceRecord, SourceRecord>> lastEventProcessor,
                                          Runnable preSnapshotAction,
                                          NotificationService<MySqlPartition, MySqlOffsetContext> notificationService,
                                          SnapshotterService snapshotterService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, metrics, lastEventProcessor,
                preSnapshotAction, notificationService, snapshotterService);
        this.connectorConfig = connectorConfig;
    }
    // 当快照开始时，需要一个上下文来记录当前的偏移量信息（Offsets）。它将传入的通用配置强制转换为 MySqlConnectorConfig，并调用 MySqlOffsetContext.initial() 来创建一个符合 MySQL 规范的初始偏移量状态。
    @Override
    protected MySqlOffsetContext getInitialOffsetContext(BinlogConnectorConfig connectorConfig) {
        return MySqlOffsetContext.initial((MySqlConnectorConfig) connectorConfig);
    }
    // 通过查询数据库，获取当前的 Binlog 文件名、Pos 位置以及 GTID 集合，并存入偏移量上下文。
    @Override
    protected void setOffsetContextBinlogPositionAndGtidDetailsForSnapshot(MySqlOffsetContext offsetContext,
                                                                           BinlogConnectorConnection connection,
                                                                           SnapshotterService snapshotterService)
            throws Exception {
        LOGGER.info("Read binlog position of MySQL primary server");
        // 1. 获取查询 Binlog 状态的 SQL 语句
        // 在 MySQL 8.0 之前通常是 "SHOW MASTER STATUS"，8.0.22 之后推荐使用 "SHOW BINARY LOG STATUS"
        final String showMasterStmt = connection.binaryLogStatusStatement();
        // 2. 执行 JDBC 查询
        connection.query(showMasterStmt, rs -> {
            if (rs.next()) {
                // 3. 提取 Binlog 文件名 (ResultSet 第 1 列，例如: mysql-bin.000001)
                final String binlogFilename = rs.getString(1);
                final long binlogPosition = rs.getLong(2);
                offsetContext.setBinlogStartPoint(binlogFilename, binlogPosition);
                // 6. 检查结果集列数。GTID 集合通常在第 5 列
                // 此列仅存在于 MySQL 5.6.5 及以上版本，且需开启 GTID 模式
                if (rs.getMetaData().getColumnCount() > 4) {
                    // This column exists only in MySQL 5.6.5 or later ...
                    final String gtidSet = rs.getString(5); // GTID set, may be null, blank, or contain a GTID set
                    offsetContext.setCompletedGtidSet(gtidSet);
                    LOGGER.info("\t using binlog '{}' at position '{}' and gtid '{}'", binlogFilename, binlogPosition,
                            gtidSet);
                }
            }
            else if (!snapshotterService.getSnapshotter().shouldStream()) {
                LOGGER.warn("Failed retrieving binlog position, continuing as streaming CDC wasn't requested");
            }
            else {
                throw new DebeziumException("Cannot read the binlog filename and position via '" + showMasterStmt
                        + "'. Make sure your server is correctly configured");
            }
        });
    }

    @Override
    protected MySqlOffsetContext copyOffset(RelationalSnapshotContext<MySqlPartition, MySqlOffsetContext> snapshotContext) {
        return new Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }
}
