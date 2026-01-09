/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.openlineage.emitter.LineageEmitter;
import io.debezium.openlineage.emitter.LineageEmitterFactory;
import io.debezium.openlineage.emitter.NoOpLineageEmitter;

/**
 * A utility class for emitting OpenLineage events from Debezium connectors.
 * <p>
 * This class serves as a facade for the underlying OpenLineage integration, providing
 * static methods for initializing the emitter and emitting lineage events at various
 * points in the Debezium connector lifecycle. The implementation uses a thread-safe
 * approach to ensure proper initialization across multiple threads.
 * <p>
 * The emitter will only be active if OpenLineage integration is enabled in the configuration.
 * Otherwise, a no-operation implementation is used that performs no actual emission.
 *
 * @see LineageEmitter
 *
 * @author Mario Fiore Vitale
 */
// DebeziumOpenLineageEmitter 是 Debezium 集成 OpenLineage 血缘追踪功能的核心门面类（Facade Class）。
// 它通过静态方法为 Debezium 连接器提供了统一的血缘事件发射入口。
// 主要作用是管理血缘发射器的生命周期，并协调血缘事件的发送。
// 集中管理：它维护了一个全局的发射器缓存（按连接器区分），确保同一个连接器的多个任务（Tasks）共用同一个发射器。
// 动态加载：利用 Java 的 ServiceLoader 机制动态加载具体的血缘工厂实现，增强了代码的插件化能力。
// 安全保护：如果配置中未启用 OpenLineage，它会自动切换到“无操作”（No-Op）模式，防止对核心 CDC 逻辑产生性能影响或报错。
public class DebeziumOpenLineageEmitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumOpenLineageEmitter.class);
    // 静态 ServiceLoader 实例。
    // 用于在运行时查找并加载 LineageEmitterFactory 的实现类
    private static final ServiceLoader<LineageEmitterFactory> lineageEmitterFactory = ServiceLoader.load(LineageEmitterFactory.class);
    // 一个同步锁对象。
    // 由于 ServiceLoader 本身不是线程安全的，当多个任务并发初始化时，通过此锁确保安全访问。
    private static final Object SERVICE_LOADER_LOCK = new Object();

    // Thread-safe map to store emitters per connector
    // 键是连接器的标识符，值是对应的 LineageEmitter。
    // 它保证了多线程环境下连接器与发射器映射关系的安全性。
    private static final ConcurrentHashMap<String, LineageEmitter> emitters = new ConcurrentHashMap<>();

    private static volatile NoOpLineageEmitter noOpLineageEmitter;

    /**
     * Emits a lineage event for the given source task state.
     *
     * @param connectorContext The connector context
     * @param state The current state of the source task
     * @throws IllegalStateException If the emitter has not been initialized for this connector
     */
    // 发送最基本的任务状态变更事件（如任务开始/结束）
    public static void emit(ConnectorContext connectorContext, DebeziumTaskState state) {
        getEmitter(connectorContext).emit(state);
    }

    /**
     * Emits a lineage event for the given source task state and exception.
     * <p>
     * This method is typically used for error reporting.
     *
     * @param connectorContext The connector context
     * @param state The current state of the source task
     * @param t The exception that occurred during processing
     * @throws IllegalStateException If the emitter has not been initialized for this connector
     */
    // 发送带有异常信息的事件。主要用于错误报告，在血缘图中标记数据链路的故障点。
    public static void emit(ConnectorContext connectorContext, DebeziumTaskState state, Throwable t) {
        getEmitter(connectorContext).emit(state, List.of(), t);
    }

    /**
     * Emits a lineage inputDatasetMetadata for the given source task state and table inputDatasetMetadata.
     * <p>
     * This method is typically used for emitting input dataset lineage.
     *
     * @param connectorContext The connector context
     * @param state The current state of the source task
     * @param datasetMetadata A list of input dataset metadata containing metadata for lineage
     * @throws IllegalStateException If the emitter has not been initialized for this connector
     */
    // 发送包含数据集（表/Topic）元数据的事件。这是血缘的核心，定义了数据节点。
    public static void emit(ConnectorContext connectorContext, DebeziumTaskState state, List<DatasetMetadata> datasetMetadata) {
        getEmitter(connectorContext).emit(state, datasetMetadata);
    }

    /**
     * Emits a lineage event for the given source task state, table event, and exception.
     * <p>
     * This method provides the most detailed lineage information, including both table
     * metadata and any exception that occurred during processing.
     *
     * @param connectorContext The connector context
     * @param state The current state of the source task
     * @param datasetMetadata A list of input dataset metadata containing metadata for lineage
     * @param t The exception that occurred during processing, may be {@code null}
     * @throws IllegalStateException If the emitter has not been initialized for this connector
     */
    // 最详细的发射方法，同时包含状态、元数据和可能发生的错误。
    public static void emit(ConnectorContext connectorContext, DebeziumTaskState state, List<DatasetMetadata> datasetMetadata, Throwable t) {
        getEmitter(connectorContext).emit(state, datasetMetadata, t);
    }
    // 根据配置、名称和运行 ID 创建 ConnectorContext 对象，作为发射器的上下文环境。
    public static ConnectorContext connectorContext(Map<String, String> config, String connectorName, UUID runId) {
        return ConnectorContext.from(config, connectorName, runId);
    }

    /**
     * Removes the emitter for the specified connector.
     * Should be called when a connector is stopped or destroyed.
     *
     * @param connectorContext The context the connector
     */
    // 关闭指定的发射器并将其从 emitters 缓存中移除。通常在连接器停止（Stop）时调用，防止内存泄漏。
    public static void cleanup(ConnectorContext connectorContext) {
        getEmitter(connectorContext).close();
        LineageEmitter removed = emitters.remove(connectorContext.toEmitterKey());
        if (removed != null) {
            LOGGER.debug("Cleaned up emitter for connector {}", connectorContext);
        }
    }
    // 核心获取逻辑。
    // 首先检查血缘开关，如果关闭则返回 NoOp 实例；如果开启则先从缓存取，取不到则调用 init。
    private static LineageEmitter getEmitter(ConnectorContext connectorContext) {
        if (isOpenLineageDisabled(connectorContext)) {
            return getNoOpLineageEmitter();
        }

        LineageEmitter emitter = emitters.get(connectorContext.toEmitterKey());
        LOGGER.debug("Available emitters {}", emitters);
        if (emitter == null) {
            return init(connectorContext);
        }
        return emitter;
    }
    // 初始化逻辑。
    // 使用 computeIfAbsent 结合 ServiceLoader 创建发射器。如果找不到任何工厂实现，则默认回退到 NoOp 模式。
    private static LineageEmitter init(ConnectorContext connectorContext) {

        LOGGER.debug("Calling init for connector with context {}", connectorContext);

        LineageEmitter emitter = emitters.computeIfAbsent(connectorContext.toEmitterKey(), key -> {
            LOGGER.debug("Creating new emitter for connector with name {}", key);
            /*
             * lineageEmitterFactory addresses native compilation issues with the Debezium Quarkus extension when OpenLineage
             * dependencies are scoped as 'provided'. In native builds, Quarkus performs comprehensive
             * classpath scanning and fails when referenced classes are unavailable at build time.
             * ServiceLoader registration signals to Quarkus that the implementation will be available
             * at runtime, allowing the native compilation to proceed successfully.
             */
            synchronized (SERVICE_LOADER_LOCK) { // This required for connectors with multiple tasks because the ServiceLoader is not thread-safe
                return lineageEmitterFactory
                        .stream()
                        .findFirst()
                        .map(ServiceLoader.Provider::get)
                        .orElse((ignore) -> new NoOpLineageEmitter())
                        .get(connectorContext);
            }
        });

        LOGGER.debug("Emitter instance for connector {}: {}", connectorContext.connectorName(), emitter);

        return emitter;
    }
    // 采用双重检查锁定（DCL）模式实现的懒加载单例，返回空实现的发射器。
    private static NoOpLineageEmitter getNoOpLineageEmitter() {
        if (noOpLineageEmitter == null) {
            synchronized (DebeziumOpenLineageEmitter.class) {
                if (noOpLineageEmitter == null) {
                    noOpLineageEmitter = new NoOpLineageEmitter();
                }
            }
        }
        return noOpLineageEmitter;
    }
    // 读取 OPEN_LINEAGE_INTEGRATION_ENABLED 配置项，判断用户是否禁用了此功能。
    private static boolean isOpenLineageDisabled(ConnectorContext connectorContext) {
        // If config is null, it means that context was created from headers, so we assume OpenLineage is enabled
        return connectorContext.config() != null && !Boolean.parseBoolean(connectorContext.config().get(OPEN_LINEAGE_INTEGRATION_ENABLED));
    }
}
