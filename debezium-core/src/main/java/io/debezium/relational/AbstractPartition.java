/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Collections;
import java.util.Map;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.LoggingContext;

/**
 * An abstract implementation of {@link io.debezium.pipeline.spi.Partition} which provides default facilities for logging.
 *
 * @author vjuranek
 */
// 标准化标识：它确立了关系型数据库分区的一个核心属性——数据库逻辑名称（Database Name）。
// 统一日志上下文：它实现了日志诊断上下文（MDC）的自动化注入。通过这个类，所有继承它的子类（如 MySqlPartition, PostgresPartition）都能在打印日志时自动带上数据库名称，
// 这对于在同一个 JVM 中运行多个连接器任务的排查工作至关重要。
public abstract class AbstractPartition implements Partition {
    // 存储当前分区的逻辑标识符（通常对应配置中的 topic.prefix 或旧版本中的 database.server.name）
    protected final String databaseName;

    public AbstractPartition(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Map<String, String> getLoggingContext() {
        return Collections.singletonMap(LoggingContext.DATABASE_NAME, databaseName);
    }
}
