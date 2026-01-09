/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service.spi;

import java.io.Closeable;

import io.debezium.common.annotation.Incubating;
import io.debezium.service.Service;
import io.debezium.service.UnknownServiceException;

/**
 * Registry of Debezium Services.
 *
 * @author Chris Cranford
 */
// ServiceRegistry 是一个非常关键的底层基础设施接口。它采用了 控制反转（IoC） 和 服务定位器（Service Locator） 的设计模式。
// ServiceRegistry 的字面意思是 “服务注册表”。
// 它的核心作用是管理 Debezium 连接器运行期间所需的各种底层服务组件（如：快照查询提供者、自定义转换器、Schema 历史管理器等）。
// 其主要价值体现为：
// 解耦：连接器的核心逻辑不需要知道某个服务是如何创建的，只需要从注册表中“索取”即可。
// 生命周期管理：通过统一的注册表，Debezium 可以集中管理服务的初始化、缓存以及在任务停止时的关闭操作。
// 可扩展性：开发者可以通过注册自定义的 ServiceProvider（服务提供者）来替换或增强 Debezium 的默认行为。
// 延迟加载（Lazy Loading）：服务通常在第一次被请求时才会被实例化，节省了系统资源。
@Incubating
public interface ServiceRegistry extends Closeable {
    /**
     * Get a service by class type.
     *
     * @param serviceClass the service class
     * @return the requested service
     * @param <T> the service class type
     * @throws UnknownServiceException if the requested service is not found
     */
    // 根据服务类的类型获取对应的服务实例
    // serviceClass 是你想要获取的服务接口或实现类的 Class 对象。
    // 请求的类必须继承自 io.debezium.service.Service
    <T extends Service> T getService(Class<T> serviceClass);

    /**
     * Safely get a service if it exists, or null if it does not.
     *
     * @param serviceClass the service class
     * @return the requested service or {@code null} if the service was not found
     * @param <T> the service class type
     */
    // 安全地尝试获取服务，不会抛出未找到异常。
    default <T extends Service> T tryGetService(Class<T> serviceClass) {
        try {
            return getService(serviceClass);
        }
        catch (UnknownServiceException e) {
            // ignored
        }
        return null;
    }

    /**
     * Register a service provider with the service registry. A provider allows the construction
     * and resolution of services lazily upon first request and use.
     *
     * @param serviceProvider the service provider, should not be {@code null}
     * @param <T> the service type
     */
    // 向注册表中添加一个新的服务提供者。
    // 参数：serviceProvider 包含如何创建服务的逻辑。
    <T extends Service> void registerServiceProvider(ServiceProvider<T> serviceProvider);

    /**
     * Closes the service registry.
     */
    @Override
    void close();
}
