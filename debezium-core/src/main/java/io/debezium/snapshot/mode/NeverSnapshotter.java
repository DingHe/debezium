/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.snapshot.mode;

import java.util.Map;

import io.debezium.spi.snapshot.Snapshotter;

/**
 * Currently only valid for MySQL. Deprecation is in evaluation for Debezium 3.0
 */
// 代表了 snapshot.mode = never（从不快照）这一配置逻辑。
// NeverSnapshotter 的核心作用是禁止所有的快照行为，直接进入增量流处理阶段。
// NeverSnapshotter 的存在告诉 Debezium 引擎：跳过第一阶段，永远不要扫描全表存量数据。它通常用于以下场景：
// 用户知道偏移量（Offsets）已经存在，或者手动维护了起始位置。
// 用户只关心从连接器启动那一刻起的新增变更，不需要历史数据。

public class NeverSnapshotter implements Snapshotter {

    @Override
    public String name() {
        return "never";
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
        return false;
    }

    @Override
    public boolean shouldSnapshotSchema(boolean offsetExists, boolean snapshotInProgress) {
        return false;
    }

    @Override
    public boolean shouldStream() {
        return true;
    }

    @Override
    public boolean shouldSnapshotOnSchemaError() {
        return false;
    }

    @Override
    public boolean shouldSnapshotOnDataError() {
        return false;
    }

}
