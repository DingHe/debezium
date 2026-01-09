/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.common;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;

// 如果说 AbstractSourceInfo 定义了“Source 应该长什么样”，那么 BaseSourceInfo 就实现了其中关于“快照状态（Snapshot Status）”的通用逻辑：
// 状态存储：它提供了一个受保护的成员变量来实际存储 SnapshotRecord 枚举值。
// 逻辑判定：它封装了判断当前记录是否属于“传统快照阶段”的逻辑。
// 统一实现：由于几乎所有 Debezium 连接器处理快照逻辑的方式都是相似的，BaseSourceInfo 通过提供 getter/setter，避免了在每个具体连接器类中重复编写快照状态管理的模板代码。
public abstract class BaseSourceInfo extends AbstractSourceInfo {

    protected SnapshotRecord snapshotRecord;

    public BaseSourceInfo(CommonConnectorConfig config) {
        super(config);
    }
    // 判断当前记录是否来自于**初始快照（Initial Snapshot）**阶段。
    public boolean isSnapshot() {
        return snapshotRecord != SnapshotRecord.INCREMENTAL && snapshotRecord != SnapshotRecord.FALSE;
    }

    /**
     * @param snapshot - TRUE if the source of even is snapshot phase, not the database log
     */
    public void setSnapshot(SnapshotRecord snapshot) {
        this.snapshotRecord = snapshot;
    }

    @Override
    public SnapshotRecord snapshot() {
        return snapshotRecord;
    }
}
