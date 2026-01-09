/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
// ErrorHandler 是整个数据流水线的安全保障中心。
// 它负责监控采集线程（Producer）中发生的异常，并根据异常的性质和配置决定是让连接器“优雅地重试”还是“直接报错停止”。
// ErrorHandler 的核心职责是管理和分类采集过程中发生的错误。
// ErrorHandler 的存在意义在于：
// 异常捕获与传递：采集线程（如读取 Binlog 的线程）发生的异常通过它传递给 Kafka Connect 主线程。
// 重试决策：通过分析异常链（Throwable Chain），判断该错误是否为“可恢复的”（如 IOException）。
// 重试计数管理：跟踪已经重试的次数，防止在无法恢复的错误上无限死循环。
// 自定义扩展：支持用户通过配置正则表达式，将特定的数据库错误信息识别为“可重试”错误。
public class ErrorHandler {
    // 表示不限制重试次数。
    public static final int RETRIES_UNLIMITED = -1;
    // 表示禁用重试，一旦报错立即停止。
    public static final int RETRIES_DISABLED = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(ErrorHandler.class);
    // 事件阻塞队列。
    // 当错误发生时，ErrorHandler 会向队列发送一个“致命异常信号”，通知消费者停止工作。
    private final ChangeEventQueue<?> queue;
    // 线程安全的异常引用。
    // 记录第一个导致生产者崩溃的异常。
    private final AtomicReference<Throwable> producerThrowable;
    // 连接器配置对象。
    // 用于读取重试次数设置和自定义重试正则。
    private final CommonConnectorConfig connectorConfig;
    // 最大重试次数。
    // 取自配置 errors.max.retries。
    private int maxRetries;
    // 当前已累计重试的次数。
    private int retries;

    public ErrorHandler(Class<? extends SourceConnector> connectorType, CommonConnectorConfig connectorConfig,
                        ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        this.connectorConfig = connectorConfig;
        this.queue = queue;
        this.producerThrowable = new AtomicReference<>();
        if (connectorConfig != null) {
            this.maxRetries = connectorConfig.getMaxRetriesOnError();
        }
        else {
            this.maxRetries = RETRIES_UNLIMITED;
        }
        if (replacedErrorHandler != null) {
            this.retries = replacedErrorHandler.getRetries();
        }
    }
    // 当采集线程崩溃时调用此方法。
    public void setProducerThrowable(Throwable producerThrowable) {
        LOGGER.error("Producer failure", producerThrowable);
        // 使用 CAS（compareAndSet）确保只记录第一个触发崩溃的异常。
        boolean first = this.producerThrowable.compareAndSet(null, producerThrowable);
        // 调用 isRetriable 和 isCustomRetriable 判断是否可重试。
        boolean retriable = isRetriable(producerThrowable);

        if (!retriable) {
            retriable = isCustomRetriable(producerThrowable);
        }

        if (first) {
            // 如果可重试且次数未超限，向队列抛出 RetriableException（Kafka Connect 会尝试重启 Task）
            if (retriable && hasMoreRetries()) {
                queue.producerException(
                        new RetriableException("An exception occurred in the change event producer. This connector will be restarted.", producerThrowable));
            }
            else {
                // 否则，向队列抛出 ConnectException（Task 进入 FAILED 状态）。
                queue.producerException(new ConnectException("An exception occurred in the change event producer. This connector will be stopped.", producerThrowable));
            }
        }
    }

    public Throwable getProducerThrowable() {
        return producerThrowable.get();
    }
    // 定义哪些异常类默认被视为可重试。默认只包含 IOException.class。
    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collections.singleton(IOException.class);
    }

    /**
     * Whether the given throwable is retriable (e.g. an exception indicating a
     * connection loss) or not.
     * By default only I/O exceptions are retriable
     */
    // 递归检查异常链，判断是否包含预定义的通信类异常（默认只有 IOException）。
    protected boolean isRetriable(Throwable throwable) {
        if (throwable == null) {
            return false;
        }
        for (Class<? extends Exception> e : communicationExceptions()) {
            if (e.isAssignableFrom(throwable.getClass())) {
                return true;
            }
        }
        return isRetriable(throwable.getCause());
    }

    /**
     * Whether the given non-retriable matches a custom retriable setting.
     *
     * @return true if non-retriable is converted to retriable
     */
    // 检查异常的 message 是否符合用户配置的正则表达式
    // 检查异常的 message 是否符合用户配置的正则表达式（custom.retriable.exception.regexp）。这在处理特定数据库 SQL 错误码时非常有用。
    protected boolean isCustomRetriable(Throwable throwable) {
        if (!connectorConfig.customRetriableException().isPresent()) {
            return false;
        }
        while (throwable != null) {
            if (throwable.getMessage() != null
                    && throwable.getMessage().matches(connectorConfig.customRetriableException().get())) {
                return true;
            }
            throwable = throwable.getCause();
        }
        return false;
    }

    /**
     * Whether the maximum number of retries has been reached
     *
     * @return true if maxRetries is -1 or retries < maxRetries
     */
    // 判断是否还能继续重试。
    protected boolean hasMoreRetries() {
        boolean doRetry = unlimitedRetries() || retries < maxRetries;
        if (doRetry) {
            retries++;
            LOGGER.warn("Retry {} of {} retries will be attempted", retries,
                    unlimitedRetries() ? "unlimited" : maxRetries);
        }
        else {
            LOGGER.error("The maximum number of {} retries has been attempted", maxRetries);
        }

        return doRetry;
    }
    // 判断 maxRetries 是否等于 -1。
    private boolean unlimitedRetries() {
        return maxRetries == RETRIES_UNLIMITED;
    }

    public int getRetries() {
        return retries;
    }

    public void resetRetries() {
        this.retries = 0;
    }
}
