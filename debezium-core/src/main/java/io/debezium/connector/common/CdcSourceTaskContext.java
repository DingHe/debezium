/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import java.util.Map;
import java.util.UUID;

import org.apache.kafka.connect.source.SourceTask;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/**
 * Contains contextual information and objects scoped to the lifecycle of Debezium's {@link SourceTask} implementations.
 *
 * @author Gunnar Morling
 */
// 在 Kafka Connect 启动一个 SourceTask 时，会有大量的配置、标识符和工具对象需要跨组件共享。该类将这些分散的信息封装成一个统一的上下文对象，其主要价值包括：
// 身份标识：存储连接器类型、逻辑名称和任务 ID，确保在多任务环境下能够区分日志和监控指标。
// 配置共享：持有解析后的强类型配置（connectorConfig）和原始配置（rawConfig）。
// 日志辅助：提供 MDC（Mapped Diagnostic Context）支持，让日志自动带上连接器名称，方便运维排查问题。

public class CdcSourceTaskContext<T extends CommonConnectorConfig> {
    // 连接器类型。例如 "MySQL", "PostgreSQL"。用于分类监控指标。
    private final String connectorType;
    // 逻辑名称。即用户配置的 name 属性（如 inventory-connector）
    private final String connectorLogicalName;
    // 插件全名。通常对应 Debezium 的内部类名标识。
    private final String connectorPluginName;
    // 任务 ID。Kafka Connect 可能会将一个 Job 拆分为多个 Task，这里标记是第几个（如 "0"）。
    private final String taskId;
    // 自定义指标标签。
    // 允许用户在 JMX 指标中注入自定义的键值对标签。
    private final Map<String, String> customMetricTags;
    // 时钟工具。
    // Debezium 封装的时间工具，用于记录事件发生时间和性能度量。
    private final Clock clock;
    // 强类型配置。这是经过解析后的、特定数据库相关的配置对象（如 MySqlConnectorConfig）
    private final T connectorConfig;
    // 原始配置。未经解析的 KV 键值对配置，用于某些需要动态读取属性的场景。
    private final Configuration rawConfig;
    // 运行 ID。每次任务启动时生成的唯一标识符，用于区分同名连接器的不同运行周期。
    private final UUID runId;

    public CdcSourceTaskContext(Configuration rawConfig,
                                T connectorConfig,
                                String taskId,
                                Map<String, String> customMetricTags) {
        this.connectorType = connectorConfig.getContextName();
        this.connectorLogicalName = connectorConfig.getLogicalName();
        this.connectorPluginName = connectorConfig.getConnectorName();
        this.taskId = taskId;
        this.customMetricTags = customMetricTags;
        this.connectorConfig = connectorConfig;
        this.rawConfig = rawConfig;
        this.clock = Clock.system();
        this.runId = UUIDUtils.generateNewUUID();
    }

    public CdcSourceTaskContext(Configuration rawConfig,
                                T connectorConfig,
                                Map<String, String> customMetricTags) {
        this(rawConfig, connectorConfig, "0", customMetricTags);
    }

    /**
     * Configure the logger's Mapped Diagnostic Context (MDC) properties for the thread making this call.
     *
     * @param contextName the name of the context; may not be null
     * @return the previous MDC context; never null
     * @throws IllegalArgumentException if {@code contextName} is null
     */
    // 设置当前线程的 MDC 日志属性
    public LoggingContext.PreviousContext configureLoggingContext(String contextName) {
        return LoggingContext.forConnector(connectorType, connectorLogicalName, contextName);
    }
    // 针对特定分区的日志上下文配置。
    // 在处理多分区任务（如多库多表同步）时，让日志明确显示当前正在处理哪个分区。
    public LoggingContext.PreviousContext configureLoggingContext(String contextName, Partition partition) {
        return LoggingContext.forConnector(connectorType, connectorLogicalName, taskId, contextName, partition);
    }

    /**
     * Run the supplied function in the temporary connector MDC context, and when complete always return the MDC context to its
     * state before this method was called.
     *
     * @param connectorConfig the configuration of the connector; may not be null
     * @param contextName the name of the context; may not be null
     * @param operation the function to run in the new MDC context; may not be null
     * @throws IllegalArgumentException if any of the parameters are null
     */
    // 在一个闭包操作中临时切换日志上下文。
    public void temporaryLoggingContext(CommonConnectorConfig connectorConfig, String contextName, Runnable operation) {
        LoggingContext.temporarilyForConnector("MySQL", connectorConfig.getLogicalName(), contextName, operation);
    }

    /**
     * Returns a clock for obtaining the current time.
     */
    public Clock getClock() {
        return clock;
    }

    public String getConnectorType() {
        return connectorType;
    }

    public String getConnectorLogicalName() {
        return connectorLogicalName;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getConnectorPluginName() {
        return connectorPluginName;
    }

    public Map<String, String> getCustomMetricTags() {
        return customMetricTags;
    }

    public T getConfig() {
        return connectorConfig;
    }

    public Configuration getRawConfig() {
        return rawConfig;
    }

    public UUID getRunId() {
        return runId;
    }
}
