/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.util.Map;
import java.util.Objects;

import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.AbstractPartition;
import io.debezium.util.Collect;

/**
 * Describes the source partition details for a binlog-based connector.
 *
 * @author Chris Cranford
 */
// 在 Kafka Connect 框架中，Partition 的作用是作为主键来查找对应的 Offset（偏移量）。BinlogPartition 的核心作用是：
// 唯一标识数据源：它明确了当前连接器正在同步哪一个逻辑服务器（由 serverName 标识）。
// 连接分区与位点：Kafka 通过 BinlogPartition 生成的 Map（例如 {"server": "my_db_server"}）来定位该服务器对应的 Binlog 文件名、位置和 GTID 等信息。
// 隔离不同实例：如果你在同一个 Kafka 集群中运行多个 MySQL 连接器，每个连接器通过不同的 serverName 来确保它们的进度（Offsets）互不干扰。

public class BinlogPartition extends AbstractPartition implements Partition {
    // 定义了存储在 Kafka Offset 消息中的 Key 键名
    // 它决定了持久化数据中“逻辑服务器名称”这一项的名称。在 Debezium 的历史版本中，这个值始终是 "server"。
    private static final String SERVER_PARTITION_KEY = "server";
    // 存储逻辑服务器的名称。
    // 通常来自于连接器配置中的 topic.prefix（新版本）或 database.server.name（旧版本）
    private final String serverName;

    public BinlogPartition(String serverName, String databaseName) {
        super(databaseName);
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BinlogPartition that = (BinlogPartition) o;
        return Objects.equals(serverName, that.serverName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverName);
    }

    @Override
    public String toString() {
        return "BinlogPartition{" +
                "serverName='" + serverName + '\'' +
                "} " + super.toString();
    }
}
