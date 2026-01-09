/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.signal;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.engine.DebeziumEngine;
import io.debezium.pipeline.signal.actions.snapshotting.CloseIncrementalSnapshotWindow;

/**
 * The class represent the signal sent on a channel:
 * <ul>
 * <li>{@code id STRING} - the unique identifier of the signal sent, usually UUID, can be used for deduplication</li>
 * <li>{@code type STRING} - the unique logical name of the code executing the signal</li>
 * <li>{@code data STRING} - the data in JSON format that are passed to the signal code
 * </ul>
 *
 * @author Mario Fiore Vitale
 */
// 在 Debezium 的信号处理机制中，SignalRecord 类是一个数据传输对象（DTO），它代表了从各种通道（如数据库信号表、Kafka Topic 等）读取到的控制指令。
// SignalRecord 的主要作用是标准化信号内容。
// Debezium 的“信号”机制允许用户在连接器运行时发送指令（例如：触发增量快照、记录日志、停止任务等）。由于这些信号可能来自不同的源（Source），SignalRecord 提供了一个统一的内部表示格式，使得后端的 SignalProcessor（信号处理器）可以忽略来源，只关注指令内容本身。

public class SignalRecord {
    // 信号唯一标识。通常是一个 UUID。它的核心作用是去重，防止同一条指令被多次重复执行。
    private final String id;
    // 指令类型。定义了要执行的具体动作。例如 execute-snapshot（执行快照）或 log（打印日志）。它决定了后续会触发哪段代码逻辑。
    private final String type;
    // 指令参数。通常是一个 JSON 字符串。例如，如果类型是快照，这里会包含要快照的表名、条件等详细配置。
    private final String data;
    // 附加元数据。存储一些非核心的上下文信息，提供扩展性，以便某些特定的信号处理逻辑使用。
    private final Map<String, Object> additionalData;

    public SignalRecord(String id, String type, String data, Map<String, Object> additionalData) {
        this.id = id;
        this.type = type;
        this.data = data;
        this.additionalData = additionalData;
    }
    // 转换构造函数。
    // 将 Debezium 引擎层定义的 Signal 对象转换为内部使用的 SignalRecord。
    public SignalRecord(DebeziumEngine.Signal signal) {
        this(signal.id(), signal.type(), signal.data(), signal.additionalData());
    }
    // 从数据库的“信号表”变更事件中解析出信号。
    public static Optional<SignalRecord> buildSignalRecordFromChangeEventSource(Struct value, CommonConnectorConfig config) {
        // DELETE 操作处理：如果监听到删除事件（DELETE），Debezium 认为这可能是一种 INSERT_DELETE 策略（为了保持信号表简洁，插入信号后立即删除）。
        // 此时它会读取 before 镜像中的数据，并将信号 ID 中的 open 替换为 close（专门用于关闭增量快照窗口的逻辑）。
        if (Envelope.Operation.DELETE.code().equals(value.get(Envelope.FieldName.OPERATION))) {
            // here we are sure the INSERT_DELETE strategy is used

            final Optional<String[]> parseSignal = config.parseSignallingMessage(value, Envelope.FieldName.BEFORE);

            return parseSignal.map(signalMessage -> {
                final String signalId = signalMessage[0].replace("open", "close");
                return new SignalRecord(signalId, CloseIncrementalSnapshotWindow.NAME, signalMessage[2], Map.of());
            });
        }

        final Optional<String[]> parseSignal = config.parseSignallingMessage(value, Envelope.FieldName.AFTER);

        return parseSignal.map(signalMessage -> new SignalRecord(signalMessage[0], signalMessage[1], signalMessage[2], Map.of()));
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getData() {
        return data;
    }

    public <T> T getAdditionalDataProperty(String property, Class<T> type) {
        return type.cast(additionalData.get(property));
    }

    public Map<String, Object> getAdditionalData() {
        return additionalData;
    }

    @Override
    public String toString() {
        return "SignalRecord{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", data='" + data + '\'' +
                ", additionalData=" + additionalData +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SignalRecord that = (SignalRecord) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
