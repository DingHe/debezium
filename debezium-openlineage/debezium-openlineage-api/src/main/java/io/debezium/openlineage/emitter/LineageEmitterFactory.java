/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage.emitter;

import io.debezium.openlineage.ConnectorContext;
// 作为数据血缘功能的核心插件点，负责创建和管理 LineageEmitter 实例
// LineageEmitterFactory 的主要作用是解耦血缘发射器的配置与创建逻辑。
// 统一抽象：它定义了一个标准化的入口，使得 Debezium 核心引擎不需要知道具体使用的是哪种血缘发射器（如 OpenLineage、Marquez 等）。
public interface LineageEmitterFactory {
    // connectorContext: 这是 ConnectorContext 类型的对象。它包含了当前连接器的所有关键配置信息、元数据定义以及运行时环境参数。
    LineageEmitter get(ConnectorContext connectorContext);
}
