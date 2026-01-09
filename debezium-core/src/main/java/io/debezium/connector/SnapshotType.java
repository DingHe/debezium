/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector;

/**
 * Describes the kind of snapshot in progress
 *
 * @author Mario Fiore Vitale
 *
 */
// 如果说 SnapshotRecord 是描述“单条数据”的标记，那么 SnapshotType 就是描述“整个过程”的模式：
// 定义任务性质：它告诉 Debezium 核心引擎，当前正在运行的扫描任务是为了初始化数据（Initial），还是因为收到了指令要补充数据（Incremental），亦或是为了某种阻塞式同步（Blocking）。
// 持久化状态：这个类型会被记录在 Kafka 的 Offset（偏移量） 中。当连接器崩溃重启后，Debezium 会读取这个值，从而知道应该恢复哪种类型的扫描逻辑。
public enum SnapshotType {
    /**
     * Indicates it is an initial snapshot.
     */
    // 初始快照
    // 当一个新的 Debezium 连接器第一次启动时，它需要把数据库中现有的所有存量数据同步到 Kafka。
    INITIAL,
    /**
     * Indicates it is a blocking snapshot.
     */
    // 阻塞式快照
    // 在某些特定的连接器配置或特定的信号触发下，需要暂时停止监听增量日志，转而全量扫描某些表。
    BLOCKING,

    /**
     * Indicates it is an incremental snapshot.
     */
    // 增量快照
    // Debezium 的高级特性（Watermarking 机制）
    // 场景：连接器已经在运行，但用户发现由于配置错误漏掉了一张表，或者某张表数据损坏需要重新同步。此时通过“信号表”发送一个指令，触发增量快照
    INCREMENTAL,
}
