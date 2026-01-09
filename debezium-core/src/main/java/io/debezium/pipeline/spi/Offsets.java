/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.spi;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import io.debezium.DebeziumException;

/**
 * Keeps track the source partitions to be processed by the connector task and their respective offsets.
 */
// Offsets 的核心作用是 统一管理一个任务（Task）负责的所有读取进度。
// 在 Kafka Connect 的模型中，一个 Connector 任务可以同时负责多个数据源分区。例如，一个 SQL Server 连接器可能在一个任务中监控多个数据库。Offsets 类通过泛型设计，将具体的“分区标识”和“位点信息”绑定在一起：
// 进度追踪：它告诉连接器从哪里开始读取（初始化时）以及当前读到了哪里（运行中）。
// 多分区支持：虽然很多 Debezium 连接器（如 MySQL）目前一个任务只处理一个分区，但 Offsets 的 Map 结构提供了处理多分区并发读取的能力。
// 类型安全：通过泛型 <P extends Partition, O extends OffsetContext>，确保了不同类型的数据库（如 MySQL 对应 MySqlPartition）能使用其特定的位点解析逻辑。
public final class Offsets<P extends Partition, O extends OffsetContext> implements Iterable<Entry<P, O>> {
    // 这是一个以 Partition 为键、OffsetContext 为值的字典。它保存了当前任务分配到的所有分区的最新状态。
    private final Map<P, O> offsets;

    private Offsets(Map<P, O> offsets) {
        this.offsets = offsets;
    }
    // 创建一个只包含单个分区和位点的 Offsets 实例。
    public static <P extends Partition, O extends OffsetContext> Offsets<P, O> of(P partition, O position) {
        Map<P, O> offsets = new HashMap<P, O>();
        offsets.put(partition, position);
        return new Offsets<>(offsets);
    }
    // 将现有的 Map 包装成 Offsets 对象。
    // 当连接器一次性初始化多个分区（如多库同步）时使用。
    public static <P extends Partition, O extends OffsetContext> Offsets<P, O> of(Map<P, O> offsets) {
        return new Offsets<>(offsets);
    }
    // 将指定分区的位点重置为 null。
    public void resetOffset(P partition) {
        offsets.put(partition, null);
    }
    // 获取当前任务管理的所有分区的集合。
    public Set<P> getPartitions() {
        return offsets.keySet();
    }
    // 获取底层的原始映射 Map。
    public Map<P, O> getOffsets() {
        return offsets;
    }

    @Override
    public Iterator<Entry<P, O>> iterator() {
        return offsets.entrySet().iterator();
    }

    /**
     * Returns the offset of the only partition that the task is configured to use.
     *
     * This method is meant to be used only by the connectors that do not implement handling
     * multiple partitions per task.
     */
    public P getTheOnlyPartition() {
        if (offsets.size() != 1) {
            throw new DebeziumException("The task must be configured to use exactly one partition, "
                    + offsets.size() + " found");
        }

        return offsets.entrySet().iterator().next().getKey();
    }

    /**
     * Returns the offset of the only offset that the task is configured to use.
     *
     * This method is meant to be used only by the connectors that do not implement handling
     * multiple partitions per task.
     */
    public O getTheOnlyOffset() {
        if (offsets.size() != 1) {
            throw new DebeziumException("The task must be configured to use exactly one partition, "
                    + offsets.size() + " found");
        }

        return offsets.entrySet().iterator().next().getValue();
    }
}
