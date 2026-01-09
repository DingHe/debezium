/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static io.debezium.relational.RelationalDatabaseConnectorConfig.DATABASE_NAME;

import java.util.Collections;
import java.util.Set;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogPartition;
import io.debezium.pipeline.spi.Partition;

// 在 Debezium 的架构中，MySqlPartition 的作用非常专一且明确：
// 身份标识 (Identification)：它代表了 Kafka Connect 框架中数据源的最小单位。对于 MySQL 而言，这通常代表一个逻辑上的 MySQL 服务器实例。
// 位点索引的 Key：Kafka Connect 在存储偏移量（Offset）时，会将 MySqlPartition 生成的内容作为 Key，将位点信息（Binlog 文件名、Pos 等）作为 Value。
// 任务分片入口：通过其内部类 Provider，它告诉 Debezium 引擎：“对于这个 MySQL 任务，你需要创建多少个分区处理逻辑。”（目前 MySQL 默认为单分区）。

public class MySqlPartition extends BinlogPartition {
    public MySqlPartition(String serverName, String databaseName) {
        super(serverName, databaseName);
    }

    public static class Provider implements Partition.Provider<MySqlPartition> {
        // 持有 MySQL 连接器的全局配置。
        private final MySqlConnectorConfig connectorConfig;
        // 持有当前 Kafka Connect 任务的具体配置。
        private final Configuration taskConfig;

        public Provider(MySqlConnectorConfig connectorConfig, Configuration taskConfig) {
            this.connectorConfig = connectorConfig;
            this.taskConfig = taskConfig;
        }
        // 返回该连接器需要处理的所有分区集合
        @Override
        public Set<MySqlPartition> getPartitions() {
            // 目前的 MySQL 连接器是一个单分区实现，即使你配置了多个数据库，它们也共享同一个逻辑分区和同一个偏移量追踪。
            return Collections.singleton(new MySqlPartition(
                    connectorConfig.getLogicalName(), taskConfig.getString(DATABASE_NAME.name())));
        }
    }
}
