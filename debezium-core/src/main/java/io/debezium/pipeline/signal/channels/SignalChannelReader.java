/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal.channels;

import java.util.List;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.signal.SignalRecord;

/**
 * This interface is used to provide custom read channels for the Debezium signaling feature:
 *
 * Implementations must:
 * define the name of the reader in {@link #name()},
 * initialize specific configuration/variables/connections in the {@link #init(CommonConnectorConfig connectorConfig)} method,
 * implement reset logic for specific channel in the {@link #reset(Object)} method if you need to reset already processed signals,
 * provide a list of signal record in the {@link #read()} method. It is called by {@link SignalProcessor} in a thread loop
 * Close all allocated resources int the {@link #close()} method.
 *
 * @author Mario Fiore Vitale
 */
// 定义了连接器如何从外部获取控制指令的标准。
// 主要作用是提供自定义的信号读取通道。
// Debezium 的“信号（Signaling）”功能允许用户在不停止连接器的情况下，通过外部干预来执行特定操作（如触发增量快照、修改日志级别等）。
// 由于不同的用户环境有不同的指令发布方式，Debezium 并没有写死读取逻辑，而是通过这个接口提供了极强的扩展性。
// 核心价值：
// 协议抽象：无论信号是存在数据库表中（JDBCOutbox）、Kafka Topic 中，还是通过 JMX 发送，只要实现了这个接口，Debezium 就能读取。
// 热插拔：基于 Java 的 ServiceLoader 机制，用户可以编写自己的实现类并打成 JAR 包放入加载目录，即可增加新的信号获取方式。
// 轮询机制：实现类负责“拉取”动作，而 SignalProcessor（信号处理器）负责在一个后台循环中不断调用这些读取器。
public interface SignalChannelReader {
    // 返回该读取器的唯一逻辑名称。
    String name();
    // 初始化读取器所需的配置、连接或资源。
    void init(CommonConnectorConfig connectorConfig);
    // 重置已处理信号的状态。
    default <T> void reset(T reference) {
    }
    // 执行实际的读取操作，返回信号列表。
    List<SignalRecord> read();

    void close();
}
