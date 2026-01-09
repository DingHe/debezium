/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Predicate;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.event.AnnotateRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.network.SSLMode;

import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogStreamingChangeEventSource;
import io.debezium.connector.binlog.jdbc.BinlogConnectorConnection;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 *
 * @author Jiri Pechanec
 */
// 如果说 Binlog... 类是一个通用的框架，那么 MySql... 类就是针对 MySQL 数据库特性的定制化实现，
// 它专门处理 MySQL 的 GTID 逻辑、SSL 连接模式以及特定的事件时间戳解析。
// MySQL 特性适配：实现 MySQL 特有的逻辑，特别是对 GTID (Global Transaction Identifier) 的精细化控制。
// 高精度时间戳提取：从 MySQL 8.0+ 的事件中提取微秒级的时间戳，提供更精确的延迟监控。
// 连接配置适配：将 Debezium 的通用安全配置转换为 MySQL 驱动识别的 SSLMode。
// 事件过滤：根据 GTID 的来源（UUID）决定是否忽略某些特定服务器产生的 DML 变更。

public class MySqlStreamingChangeEventSource extends BinlogStreamingChangeEventSource<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlStreamingChangeEventSource.class);
    // MySQL 专用的配置对象。包含了 MySQL 特有的配置项，如 GTID 过滤器、连接模式等。
    private final MySqlConnectorConfig connectorConfig;
    // 核心属性。类型为 com.github.shyiko.mysql.binlog.GtidSet。
    // 它代表了当前已处理的 GTID 集合，用于在高可用（HA）环境下确保数据不丢不重。
    private GtidSet gtidSet;

    public MySqlStreamingChangeEventSource(MySqlConnectorConfig connectorConfig,
                                           BinlogConnectorConnection connection,
                                           EventDispatcher<MySqlPartition, TableId> dispatcher,
                                           ErrorHandler errorHandler,
                                           Clock clock,
                                           MySqlTaskContext taskContext,
                                           MySqlDatabaseSchema schema,
                                           MySqlStreamingChangeEventSourceMetrics metrics,
                                           SnapshotterService snapshotterService,
                                           BinaryLogClient binaryLogClient) {
        super(connectorConfig, connection, dispatcher, errorHandler, clock, taskContext, schema, metrics, snapshotterService, binaryLogClient);
        this.connectorConfig = connectorConfig;
    }

    @Override
    protected void setEventTimestamp(Event event, long eventTs) {
        if (eventTimestamp == null || !isGtidModeEnabled()) {
            // Fallback to second resolution event timestamps
            eventTimestamp = Instant.ofEpochMilli(eventTs);
        }
        else if (event.getHeader().getEventType() == EventType.GTID) {
            // Prefer higher resolution replication timestamps from MySQL 8 GTID events, if possible
            GtidEventData gtidEvent = unwrapData(event);
            final long gtidEventTs = gtidEvent.getOriginalCommitTimestamp();
            if (gtidEventTs != 0) {
                // >= MySQL 8.0.1, prefer the higher resolution replication timestamp
                eventTimestamp = Instant.EPOCH.plus(gtidEventTs, ChronoUnit.MICROS);
            }
            else {
                // Fallback to second resolution event timestamps
                eventTimestamp = Instant.ofEpochMilli(eventTs);
            }
        }
    }

    /**
     * Handle the supplied event with a {@link GtidEventData} that signals the beginning of a GTID transaction.
     * We don't yet know whether this transaction contains any events we're interested in, but we have to record
     * it so that we know the position of this event and know we've processed the binlog to this point.
     * <p>
     * Note that this captures the current GTID and complete GTID set, regardless of whether the connector is
     * {@link MySqlConnectorConfig#getGtidSourceFilter() filtering} the GTID set upon connection. We do this because
     * we actually want to capture all GTID set values found in the binlog, whether or not we process them.
     * However, only when we connect do we actually want to pass to MySQL only those GTID ranges that are applicable
     * per the configuration.
     *
     * @param partition the partition; never null
     * @param offsetContext the offset context; never null
     * @param event the GTID event to be processed; may not be null
     * @param gtidSourceFilter the GTID source filter
     */
    @Override
    protected void handleGtidEvent(MySqlPartition partition, MySqlOffsetContext offsetContext, Event event,
                                   Predicate<String> gtidSourceFilter) {
        LOGGER.debug("GTID transaction: {}", event);
        GtidEventData gtidEvent = unwrapData(event);
        String gtid = gtidEvent.getGtid();
        gtidSet.add(gtid);
        offsetContext.startGtid(gtid, gtidSet.toString()); // rather than use the client's GTID set
        setIgnoreDmlEventByGtidSource(false);
        if (gtidSourceFilter != null && gtid != null) {
            String uuid = gtid.trim().substring(0, gtid.indexOf(":"));
            if (!gtidSourceFilter.test(uuid)) {
                setIgnoreDmlEventByGtidSource(true);
            }
        }
        setGtidChanged(gtid);
    }

    /**
     * Handle the supplied event with an {@link RowsQueryEventData} or {@link AnnotateRowsEventData} by
     * recording the original SQL query that generated the event.
     *
     * @param event the database change data event to be processed; may not be null
     */
    @Override
    protected void handleRecordingQuery(MySqlOffsetContext offsetContext, Event event) {
        final EventData eventData = unwrapData(event);
        if (eventData instanceof RowsQueryEventData) {
            final String query = ((RowsQueryEventData) eventData).getQuery();
            offsetContext.setQuery(query);
        }
    }

    @Override
    public void init(MySqlOffsetContext offsetContext) {
        setEffectiveOffsetContext(offsetContext != null ? offsetContext : MySqlOffsetContext.initial(connectorConfig));
    }

    @Override
    protected Class<? extends SourceConnector> getConnectorClass() {
        return MySqlConnector.class;
    }

    @Override
    protected EventType getIncludeQueryEventType() {
        return EventType.ROWS_QUERY;
    }

    @Override
    protected EventType getGtidEventType() {
        return EventType.GTID;
    }

    @Override
    protected void initializeGtidSet(String value) {
        this.gtidSet = new GtidSet(value);
    }

    @Override
    protected SSLMode sslModeFor(BinlogConnectorConfig.SecureConnectionMode mode) {
        switch ((MySqlConnectorConfig.MySqlSecureConnectionMode) mode) {
            case DISABLED:
                return SSLMode.DISABLED;
            case PREFERRED:
                return SSLMode.PREFERRED;
            case REQUIRED:
                return SSLMode.REQUIRED;
            case VERIFY_CA:
                return SSLMode.VERIFY_CA;
            case VERIFY_IDENTITY:
                return SSLMode.VERIFY_IDENTITY;
        }
        return null;
    }

}
