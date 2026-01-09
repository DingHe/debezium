/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.util.Optional;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.pipeline.spi.OffsetContext;

// 实现了 OffsetContext 接口，并为几乎所有具体数据库连接器（如 MySQL, Postgres, Oracle 等）提供了通用的偏移量管理逻辑。
// 核心作用是管理快照状态机的切换以及维护 SourceInfo 结构
// 状态转换中心：它定义了连接器如何在“未快照”、“正在快照”、“快照完成”以及“增量快照”这些状态之间进行转换。它通过一组组合逻辑（snapshot 类型 + snapshotCompleted 标志）来精确描述当前的运行阶段。

public abstract class CommonOffsetContext<T extends BaseSourceInfo> implements OffsetContext {
    // 定义了存储在 Kafka Offset 中的持久化 Key 名称。
    // 当连接器停止并重启时，Debezium 会通过检查 Offset Map 中这个 Key 的值来判断上次运行是否已经完成了全量快照。
    public static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";
    // 代表具体的源信息结构
    // 所有的位置信息（如文件名、Lsn、位点）和快照标记最终都会委托给这个对象来生成最终的 Struct
    protected final T sourceInfo;

    /**
     * Indicates the type of in progress snapshot (INITIAL, BLOCKING, INCREMENTAL).
     * In case of an INITIAL or BLOCKING snapshot it will be used in conjunction with {@link #snapshotCompleted}
     * to define if it is running or not.
     * <p>
     * The following table lists the possible status:
     * <table border="3">
     * <tr><th>Status</th><th>snapshot</th><th>snapshotCompleted</th></tr>
     * <tr><td>incomplete initial snapshot</td><td>initial</td><td>false</td></tr>
     * <tr><td>completed initial snapshot</td><td>null</td><td>true</td></tr>
     * <tr><td>incomplete blocking snapshot</td><td>blocking</td><td>false</td></tr>
     * <tr><td>completed blocking snapshot</td><td>null</td><td>true</td></tr>
     * <tr><td>running incremental snapshot</td><td>incremental</td><td>true</td></tr>
     * <tr><td>completed incremental snapshot</td><td>null</td><td>true</td></tr>
     * </table>
     *
     */
    // 记录当前正在进行中的快照类型（INITIAL, BLOCKING, 或 INCREMENTAL）
    // 一旦快照完成进入流处理阶段，该属性通常被设置为 null。
    protected SnapshotType snapshot;

    /**
     * Whether an initial/blocking snapshot has been completed or not.
     */
    // 指示“初始快照”或“阻塞式快照”是否已经结束
    protected boolean snapshotCompleted;

    public CommonOffsetContext(T sourceInfo) {
        this.sourceInfo = sourceInfo;
    }

    public CommonOffsetContext(T sourceInfo, boolean snapshotCompleted) {
        this.sourceInfo = sourceInfo;
        this.snapshotCompleted = snapshotCompleted;
    }
    // 生成 Kafka 消息中的 source 结构体。它直接调用 sourceInfo.struct()
    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public void markSnapshotRecord(SnapshotRecord record) {
        sourceInfo.setSnapshot(record);
    }

    @Override
    public boolean isInitialSnapshotRunning() {
        return getSnapshot().isPresent() &&
                getSnapshot().get().equals(SnapshotType.INITIAL) &&
                !snapshotCompleted;
    }

    @Override
    public void preSnapshotStart(boolean onDemand) {
        snapshot = onDemand ? SnapshotType.BLOCKING : SnapshotType.INITIAL;
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        snapshotCompleted = false;
    }

    @Override
    public void preSnapshotCompletion() {
        snapshotCompleted = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
        snapshot = null;
    }

    @Override
    public void incrementalSnapshotEvents() {
        sourceInfo.setSnapshot(SnapshotRecord.INCREMENTAL);
        snapshot = SnapshotType.INCREMENTAL;
    }

    public Optional<SnapshotType> getSnapshot() {
        return Optional.ofNullable(snapshot);
    }

    public void setSnapshot(SnapshotType snapshotType) {
        snapshot = snapshotType;
    }
}
