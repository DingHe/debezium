/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describes the source partition to be processed by the connector in connector-specific terms
 * and provides its representation as a Kafka Connect source partition.
 */
// 在 Kafka Connect 框架中，数据同步被抽象为：从一个“分区（Partition）”读取数据，并记录其“偏移量（Offset）”。
// 标识数据源：它定义了当前连接器正在“盯着”哪一个具体的物理或逻辑数据源。对于关系型数据库，一个 Partition 通常对应一个特定的数据库集群或逻辑服务器名称。
// 多版本兼容：它提供了处理旧版本分区格式的能力（Legacy Formats），确保连接器升级后依然能识别旧的位点信息。
// 任务分片基础：它是 Partition.Provider 的基础，允许 Debezium 将一个大的同步任务拆分为多个分区并行处理（虽然目前大多数数据库连接器仍是单分区运行）。


public interface Partition {

    /**
     * Get source partition representation in current format, the most recent one.
     *
     * @return map being representation of this source partition
     */
    // 获取当前最新的源分区描述。
    // 在 MySQL 连接器中，它通常返回 {"server": "my-database-server"}。这个 Key 和 Value 会被 Kafka 用来索引和存储对应的 Offset。
    Map<String, String> getSourcePartition();

    /**
     * Get all representations of the source partition in all supported formats.
     * The list includes current format, as the first one - the most preferred, but also
     * all legacy formats that are still supported, in descending order of preference.
     *
     * @return list of maps, each map being representation of this source partition
     */
    // 获取所有支持的分区表示格式，用于向后兼容。
    default List<Map<String, String>> getSupportedFormats() {
        return Collections.singletonList(getSourcePartition());
    }

    /**
     * Returns the partition representation in the logging context.
     */
    // 获取用于日志打印的上下文信息。
    default Map<String, String> getLoggingContext() {
        return Collections.emptyMap();
    }

    /**
     * Implementations provide a set of connector-specific partitions based on the connector task configuration.
     */
    // 分区提供者，负责根据连接器的任务配置（Task Config）实例化出所有的分区对象。
    interface Provider<P extends Partition> {
        Set<P> getPartitions();
    }
}
