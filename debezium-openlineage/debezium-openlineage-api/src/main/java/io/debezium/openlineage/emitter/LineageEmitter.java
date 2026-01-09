/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.emitter;

import java.util.List;

import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.openlineage.dataset.DatasetMetadata;
// LineageEmitter 是一个专门为 数据血缘（Data Lineage） 设计的接口。它主要用于将 Debezium 捕获到的元数据信息对接至 OpenLineage 等标准血缘追踪系统
// 核心作用是发送（发射）数据血缘事件。
public interface LineageEmitter {
    // 发送一个仅包含任务状态的基本血缘事件。
    void emit(DebeziumTaskState state);
    // 发送一个带有错误信息的血缘事件。
    void emit(DebeziumTaskState state, Throwable t);
    // 发送包含具体数据集信息的血缘事件。
    void emit(DebeziumTaskState state, List<DatasetMetadata> datasetMetadata);
    // 包含状态、元数据及异常。
    // 用于报告在处理特定数据集过程中发生的错误。
    void emit(DebeziumTaskState state, List<DatasetMetadata> datasetMetadata, Throwable t);

    void close();
}
