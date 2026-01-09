/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.spi;
// ChangeEventSource 接口（及其上下文接口）的主要作用是定义数据读取的行为规范和运行时的状态控制机制。
// 行为抽象：它是 SnapshotChangeEventSource（快照源）和 StreamingChangeEventSource（增量流源）的共同父接口。这使得 ChangeEventSourceCoordinator（协调器）可以用统一的方式处理不同阶段的数据采集。
public interface ChangeEventSource {
    // 用于在任务执行过程中检查外部指令或报告自身状态。
    interface ChangeEventSourceContext {

        /**
         * Whether this source is paused.
         */
        // 检查当前的事件源是否被请求暂停。
        // 在循环读取数据库日志时，读取线程会定期检查此状态。如果返回 true，读取线程通常会进入等待状态。
        boolean isPaused();

        /**
         * Whether this source is running or has been requested to stop.
         */
        // 检查当前的事件源是否应该继续运行。
        // 这是读取任务的主循环条件。如果用户停止了连接器，此方法将返回 false，从而允许读取线程优雅地退出循环并关闭数据库连接
        boolean isRunning();

        /**
         * Called to indicate that the snapshot has been completed and that streaming should therefore continue.
         */
        void resumeStreaming() throws InterruptedException;

        /**
         * Wait for the resumeStreaming function to be called, which indicates that a snapshot is done
         * and that streaming should resume.
         */
        void waitSnapshotCompletion() throws InterruptedException;

        /**
         * Called by the StreamingChangeEventSource to indicate that the streaming has now been paused, and
         * that no streaming records are being processed anymore.
         */
        void streamingPaused();

        /**
         * Wait for the streamingPaused function to be called.
         */
        void waitStreamingPaused() throws InterruptedException;
    }
}
