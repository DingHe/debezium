/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.binlog;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Strings;

/**
 * A concrete implementation of the {@link CommonOffsetContext} for binlog-based connectors that tracks the
 * current offset within the source database change stream. This reflects the committed offset state and
 * the "{@code source}" information {@link org.apache.kafka.connect.data.Struct} in the change events.
 *
 * @author Chris Cranford
 */
// 在 MySQL CDC 场景中，仅记录文件名和位置是不够的。该类的主要作用包括：
// 多维度位点追踪：同时支持 GTID（全局事务 ID）模式和 文件名+偏移量（File & Pos） 模式。
// 事务粒度恢复：追踪记录在事务内的位置。如果连接器在事务中间崩溃，它可以精确地跳过已处理的事件（restartEventsToSkip）或行（restartRowsToSkip）。
// 状态持久化：将内存中的 binlog 坐标转换为 Kafka Connect 要求的 Map 格式，用于写入 Kafka 的内部 offset topic。

public class BinlogOffsetContext<T extends BinlogSourceInfo> extends CommonOffsetContext<T> {
    // 存储在 Offset 中，记录重启后需要跳过的事件数量。
    public static final String EVENTS_TO_SKIP_OFFSET_KEY = "event";
    // 事件在数据库发生的秒级时间戳
    public static final String TIMESTAMP_KEY = "ts_sec";
    // 存储已处理完成的 GTID 集合字符串
    public static final String GTID_SET_KEY = "gtids";
    // 用于在非 GTID 模式下生成模拟事务 ID。
    public static final String NON_GTID_TRANSACTION_ID_FORMAT = "file=%s,pos=%s";

    private final Schema sourceInfoSchema;
    // 存储 Debezium 特有的事务元数据（如事务内事件计数）
    private final TransactionContext transactionContext;
    // 管理增量快照的状态（如当前扫描到的主键位置）
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
    // 分别存储“重启点”和“当前点”的 GTID 集合
    private String restartGtidSet;
    private String currentGtidSet;
    // 连接器重启时应开始读取的二进制日志文件名和位置。
    private String restartBinlogFilename;
    private long restartBinlogPosition = 0L;
    // 在一个多行变更（Row Event）中，重启后需要跳过的行数。
    private int restartRowsToSkip = 0;
    // 在同一个事务或同一个位点中，重启后需要跳过的事件数。
    private long restartEventsToSkip = 0;
    // 当前正在处理的 binlog 事件的大小（用于计算下一个位点）。
    private long currentEventLengthInBytes = 0;
    // 标记当前连接器是否正在处理一个事务块（BEGIN 到 COMMIT 之间）
    private boolean inTransaction = false;
    // 当前事务的唯一标识符（GTID 或 文件位点组合）
    private String transactionId = null;

    public BinlogOffsetContext(SnapshotType snapshot, boolean snapshotCompleted, TransactionContext transactionContext,
                               IncrementalSnapshotContext<TableId> incrementalSnapshotContext, T sourceInfo) {
        super(sourceInfo, snapshotCompleted);
        this.sourceInfoSchema = sourceInfo.schema();

        if (this.snapshotCompleted) {
            postSnapshotCompletion();
        }
        else {
            setSnapshot(snapshot);
            sourceInfo.setSnapshot(snapshot != null ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }

        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }
    // 最核心方法。生成用于持久化的 Map
    // 根据当前是否在快照期，决定返回快照偏移量还是包含事务/增量快照信息的流处理偏移量。
    @Override
    public Map<String, ?> getOffset() {
        final Map<String, Object> offset = offsetUsingPosition(restartRowsToSkip);
        if (getSnapshot().isPresent()) {
            offset.put(AbstractSourceInfo.SNAPSHOT_KEY, getSnapshot().get().toString());
        }

        return sourceInfo.isSnapshot() ? offset : incrementalSnapshotContext.store(transactionContext.store(offset));
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public void event(DataCollectionId tableId, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.tableEvent((TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    public String getTransactionId() {
        return this.transactionId;
    }

    public void setInitialSkips(long restartEventsToSkip, int restartRowsToSkip) {
        this.restartEventsToSkip = restartEventsToSkip;
        this.restartRowsToSkip = restartRowsToSkip;
    }

    public void databaseEvent(String database, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.databaseEvent(database);
        sourceInfo.tableEvent((TableId) null);
    }

    public void tableEvent(String database, Set<TableId> tableIds, Instant timestamp) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.databaseEvent(database);
        sourceInfo.tableEvent(tableIds);
    }

    /**
     * Set the position in the MySQL binlog where we will start reading.
     *
     * @param binlogFilename the name of the binary log file; may not be null
     * @param positionOfFirstEvent the position in the binary log file to begin processing
     */
    public void setBinlogStartPoint(String binlogFilename, long positionOfFirstEvent) {
        assert positionOfFirstEvent >= 0;
        if (binlogFilename != null) {
            sourceInfo.setBinlogPosition(binlogFilename, positionOfFirstEvent);
            this.restartBinlogFilename = binlogFilename;
        }
        else {
            sourceInfo.setBinlogPosition(sourceInfo.getCurrentBinlogFilename(), positionOfFirstEvent);
        }
        this.restartBinlogPosition = positionOfFirstEvent;
        this.restartRowsToSkip = 0;
        this.restartEventsToSkip = 0;
    }

    /**
     * Set the GTID set that captures all of the GTID transactions that have been completely processed.
     *
     * @param gtidSet the string representation of the GTID set; may not be null, but may be an empty string if no GTIDs
     *            have been previously processed
     */
    public void setCompletedGtidSet(String gtidSet) {
        if (gtidSet != null && !gtidSet.trim().isEmpty()) {
            // Remove all the newline chars that exist in the GTID set string ...
            String trimmedGtidSet = gtidSet.replace("\n", "").replace("\r", "");
            this.currentGtidSet = trimmedGtidSet;
            this.restartGtidSet = trimmedGtidSet;
        }
    }

    /**
     * Get the string representation of the GTID range for the MySQL binary log file.
     *
     * @return the string representation of the binlog GTID ranges; may be null
     */
    public String gtidSet() {
        return this.currentGtidSet;
    }

    /**
     * Record that a new GTID transaction has been started and has been included in the set of GTIDs known to the MySQL server.
     *
     * @param gtid the string representation of a specific GTID that has been begun; may not be null
     * @param gtidSet the string representation of GTID set that includes the newly begun GTID; may not be null
     */
    public void startGtid(String gtid, String gtidSet) {
        sourceInfo.startGtid(gtid);
        if (gtidSet != null && !gtidSet.trim().isEmpty()) {
            // Remove all the newline chars that exist in the GTID set string ...
            String trimmedGtidSet = gtidSet.replace("\n", "").replace("\r", "");
            // Set the GTID set that we'll use if restarting BEFORE successful completion of the events in this GTID ...
            this.restartGtidSet = Strings.defaultIfBlank(this.currentGtidSet, trimmedGtidSet);
            // Record the GTID set that includes the current transaction ...
            this.currentGtidSet = trimmedGtidSet;
        }
    }

    public T getSource() {
        return sourceInfo;
    }
    // 当监听到 BEGIN 事件时调用
    // 行为：重置跳过的行/事件计数，标记 inTransaction 为 true，记录当前的起始位点。
    public void startNextTransaction() {
        // If we have to restart, then we'll start with this BEGIN transaction
        this.restartRowsToSkip = 0;
        this.restartEventsToSkip = 0;
        this.restartBinlogFilename = sourceInfo.binlogFilename();
        this.restartBinlogPosition = sourceInfo.binlogPosition();
        this.inTransaction = true;
        setTransactionId();
    }
    // 当监听到 COMMIT 或 XID 事件时调用。
    // 行为：将当前位点确认为“安全重启点”，更新 restartGtidSet，重置事务标记。
    public void commitTransaction() {
        this.restartGtidSet = this.currentGtidSet;
        this.restartBinlogFilename = sourceInfo.binlogFilename();
        this.restartBinlogPosition = sourceInfo.binlogPosition() + this.currentEventLengthInBytes;
        this.restartRowsToSkip = 0;
        this.restartEventsToSkip = 0;
        this.inTransaction = false;
        sourceInfo.setQuery(null);
        resetTransactionId();
    }

    /**
     * Capture that we're starting a new event.
     */
    public void completeEvent() {
        ++restartEventsToSkip;
    }

    /**
     * Set the position within the MySQL binary log file of the <em>current event</em>.
     *
     * @param positionOfCurrentEvent the position within the binary log file of the current event
     * @param eventSizeInBytes the size in bytes of this event
     */
    public void setEventPosition(long positionOfCurrentEvent, long eventSizeInBytes) {
        sourceInfo.setEventPosition(positionOfCurrentEvent);
        this.currentEventLengthInBytes = eventSizeInBytes;
        if (!inTransaction) {
            this.restartBinlogPosition = positionOfCurrentEvent + eventSizeInBytes;
            this.restartRowsToSkip = 0;
            this.restartEventsToSkip = 0;
        }
        // Don't set anything else, since the row numbers are set in the offset(int,int) method called at least once
        // for each processed event
    }

    /**
     * Set the original SQL query.
     *
     * @param query the original SQL query that generated the event.
     */
    public void setQuery(final String query) {
        sourceInfo.setQuery(query);
    }

    public void changeEventCompleted() {
        this.restartRowsToSkip = 0;
    }

    /**
     * Get the number of events after the last transaction BEGIN that we've already processed.
     *
     * @return the number of events in the transaction that have been processed completely
     * @see #completeEvent()
     * @see #startNextTransaction()
     */
    public long eventsToSkipUponRestart() {
        return restartEventsToSkip;
    }

    /**
     * Get the number of rows beyond the {@link #eventsToSkipUponRestart() last completely processed event} to be skipped
     * upon restart.
     *
     * @return the number of rows to be skipped
     */
    public int rowsToSkipUponRestart() {
        return restartRowsToSkip;
    }

    /**
     * Given the row number within a binlog event and the total number of rows in that event, compute the
     * Kafka Connect offset that is to be included in the produced change event describing the row.
     * <p>
     * This method should always be called before {@link BinlogSourceInfo#struct()}.
     *
     * @param eventRowNumber the 0-based row number within the event for which the offset is to be produced
     * @param totalNumberOfRows the total number of rows within the event being processed
     * @see BinlogSourceInfo#struct()
     */
    // 处理多行变更事件中的行级追踪
    // 如果当前不是最后一行，则 restartRowsToSkip 设置为下一行索引，确保重启后不重复消费已写入 Kafka 的行。
    public void setRowNumber(int eventRowNumber, int totalNumberOfRows) {
        sourceInfo.setRowNumber(eventRowNumber);
        if (eventRowNumber < (totalNumberOfRows - 1)) {
            // This is not the last row, so our offset should record the next row to be used ...
            this.restartRowsToSkip = eventRowNumber + 1;
            // so write out the offset with the position of this event
        }
        else {
            // This is the last row, so write out the offset that has the position of the next event ...
            this.restartRowsToSkip = totalNumberOfRows;
        }
    }

    public void setBinlogServerId(long serverId) {
        sourceInfo.setBinlogServerId(serverId);
    }

    public void setBinlogThread(long threadId) {
        sourceInfo.setBinlogThread(threadId);
    }

    private void setTransactionId() {
        // use GTID if it is available
        if (sourceInfo.getCurrentGtid() != null) {
            this.transactionId = sourceInfo.getCurrentGtid();
        }
        else {
            this.transactionId = String.format(NON_GTID_TRANSACTION_ID_FORMAT,
                    this.restartBinlogFilename, this.restartBinlogPosition);
        }
    }

    private void resetTransactionId() {
        transactionId = null;
    }
    // 将所有的 binlog 坐标（GTID, File, Pos, Skip）封装进 Map
    private Map<String, Object> offsetUsingPosition(long rowsToSkip) {
        final Map<String, Object> map = new HashMap<>();
        if (sourceInfo.getServerId() != 0) {
            map.put(BinlogSourceInfo.SERVER_ID_KEY, sourceInfo.getServerId());
        }
        if (restartGtidSet != null) {
            // Put the previously-completed GTID set in the offset along with the event number ...
            map.put(GTID_SET_KEY, restartGtidSet);
        }
        map.put(BinlogSourceInfo.BINLOG_FILENAME_OFFSET_KEY, restartBinlogFilename);
        map.put(BinlogSourceInfo.BINLOG_POSITION_OFFSET_KEY, restartBinlogPosition);
        if (restartEventsToSkip != 0) {
            map.put(EVENTS_TO_SKIP_OFFSET_KEY, restartEventsToSkip);
        }
        if (rowsToSkip != 0) {
            map.put(BinlogSourceInfo.BINLOG_ROW_IN_EVENT_OFFSET_KEY, rowsToSkip);
        }
        if (sourceInfo.timestamp() != null) {
            map.put(TIMESTAMP_KEY, sourceInfo.timestamp().getEpochSecond());
        }
        return map;
    }

    public static abstract class Loader<O extends BinlogOffsetContext> implements OffsetContext.Loader<O> {
        protected static boolean isTrue(Map<String, ?> offset, String key) {
            return Boolean.TRUE.equals(offset.get(key)) || "true".equals(offset.get(key));
        }
    }

    @Override
    public String toString() {
        return "BinlogOffsetContext{" +
                "sourceInfoSchema=" + sourceInfoSchema +
                ", sourceInfo=" + sourceInfo +
                ", snapshotCompleted=" + snapshotCompleted +
                ", transactionContext=" + transactionContext +
                ", restartGtidSet='" + restartGtidSet + '\'' +
                ", currentGtidSet='" + currentGtidSet + '\'' +
                ", restartBinlogFilename='" + restartBinlogFilename + '\'' +
                ", restartBinlogPosition=" + restartBinlogPosition +
                ", restartRowsToSkip=" + restartRowsToSkip +
                ", restartEventsToSkip=" + restartEventsToSkip +
                ", currentEventLengthInBytes=" + currentEventLengthInBytes +
                ", inTransaction=" + inTransaction +
                ", transactionId='" + transactionId + '\'' +
                ", incrementalSnapshotContext=" + incrementalSnapshotContext +
                "}";
    }
}
