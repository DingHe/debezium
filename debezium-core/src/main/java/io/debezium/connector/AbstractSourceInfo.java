/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector;

import java.time.Instant;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;

/**
 * Common information provided by all connectors in either source field or offsets
 *
 * @author Jiri Pechanec
 */
// 定义了 CDC（数据变更捕获）消息中 "source"（源） 字段的标准结构。
// 在 Debezium 生成的每一条 Kafka 消息中，通常包含 before、after 和 source 三部分。source 部分用于描述该条变更记录的元数据起源。
// 核心作用包括：
// 标准化元数据格式：确保所有 Debezium 连接器（MySQL, PostgreSQL, Oracle 等）生成的源头信息都包含一组共同的字段（如版本、连接器名称、时间戳）。
// 支撑数据血缘与偏移量追踪：它记录了事件是在哪个数据库、哪个表、什么时间发生的，以及它是属于“快照（Snapshot）”阶段还是“流（Streaming）”阶段。
// 解耦具体的 Schema 生成逻辑：通过 SourceInfoStructMaker 接口，它允许不同的连接器灵活地构建符合 Kafka Connect 规范的 Struct 对象。
public abstract class AbstractSourceInfo {
    // 定义了输出到 Kafka JSON 消息中 source 结构体的 Key 名称

    public static final String DEBEZIUM_VERSION_KEY = "version";
    public static final String DEBEZIUM_CONNECTOR_KEY = "connector";
    public static final String SERVER_NAME_KEY = "name";
    public static final String TIMESTAMP_KEY = "ts_ms";
    public static final String TIMESTAMP_US_KEY = "ts_us";
    public static final String TIMESTAMP_NS_KEY = "ts_ns";
    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String DATABASE_NAME_KEY = "db";
    public static final String SCHEMA_NAME_KEY = "schema";
    public static final String TABLE_NAME_KEY = "table";
    public static final String COLLECTION_NAME_KEY = "collection";
    public static final String SEQUENCE_KEY = "sequence";
    // 持有连接器的通用配置对象。它被用来获取逻辑服务名、获取 SourceInfoStructMaker（负责生成 Schema 的工具）等
    private final CommonConnectorConfig config;

    protected AbstractSourceInfo(CommonConnectorConfig config) {
        this.config = config;
    }

    /**
     * Returns the schema of specific sub-types. Implementations should call
     * {@link #schemaBuilder()} to add all shared fields to their schema.
     */
    // 返回该 source 字段对应的 Kafka Connect Schema 对象
    public Schema schema() {
        return config.getSourceInfoStructMaker().schema();
    }
    // 从配置中获取 SourceInfoStructMaker 实例。它是构建 Schema 和 Struct 的工厂类。
    protected SourceInfoStructMaker<AbstractSourceInfo> structMaker() {
        return config.getSourceInfoStructMaker();
    }

    /**
     * @return timestamp of the event
     */
    // 返回事件在源数据库中发生的具体时间
    protected abstract Instant timestamp();

    /**
     * @return status whether the record is from snapshot or streaming phase
     */
    // 指示当前记录是处于全量快照阶段、增量快照阶段还是正常的 binlog 流处理阶段。
    protected abstract SnapshotRecord snapshot();

    /**
     * @return name of the database
     */
    // 返回当前事件所属的数据库名称。
    protected abstract String database();

    /**
     * @return logical name of the server
     */
    // 获取连接器的逻辑名称。默认直接从 config.getLogicalName() 获取
    protected String serverName() {
        return config.getLogicalName();
    }

    /**
     * Returns the {@code source} struct representing this source info.
     */
    // 将当前对象实例中的数据转换成 Kafka Connect 的 Struct 对象
    // 调用 structMaker().struct(this)。这是真正填充数据的地方，将内存中的属性转换为可以序列化发送到 Kafka 的格式。
    public Struct struct() {
        return structMaker().struct(this);
    }

    /**
     * Returns extra sequencing metadata about a change event formatted
     * as a stringified JSON array. The metadata contained in a sequence must be
     * ordered sequentially in order to be understood and compared.
     *
     * Note: if a source's sequence metadata contains any string values, those
     * strings must be correctly escaped before being included in the stringified
     * JSON array.
     */
    // 返回额外的序列元数据（JSON 数组字符串）。默认返回 null。
    // 当时间戳不足以完全区分事件顺序时（例如高并发下同一毫秒有多个变更），可以使用此字段来提供额外的排序依据。
    protected String sequence() {
        return null;
    };

}
