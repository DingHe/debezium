/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import io.debezium.service.spi.ServiceProvider;

/**
 * Describes a registration for a specific service.
 *
 * @author Chris Cranford
 */
// ServiceRegistration 的主要作用是描述并追踪一个服务的注册信息。
// 在 DefaultServiceRegistry（服务注册表）中，并不是直接存储服务对象，而是存储一个个 ServiceRegistration 对象。它的核心价值在于支持以下两种服务模式：
// 即时模式（Eager）：服务实例已经存在，直接将其注册。
// 延迟模式（Lazy）：只提供一个“生产工厂”（ServiceProvider），只有当真正有人需要使用该服务时，才通过该工厂去创建实例。
public final class ServiceRegistration<T extends Service> {
    // 服务类型标识。
    // 记录该服务对应的接口或实现类的类型（例如 SchemaHistory.class）。
    // 它是注册表进行查找时的“唯一键”。
    private final Class<T> serviceClass;
    // 服务提供者（工厂）。
    // 如果服务是延迟加载的，这里保存了如何创建该服务的逻辑。如果是即时注册的，则此项为 null。
    private final ServiceProvider<T> serviceProvider;
    // 服务实例缓存。
    // 一旦服务被创建或注入，其实例会保存在这里。使用 volatile 关键字确保了在多线程环境下（如 Kafka Connect 的不同线程），该实例的可见性是安全的。
    private volatile T service;

    /**
     * Create a service registration for an already existing service instance.
     *
     * @param serviceClass the service class
     * @param service the service instance
     */
    public ServiceRegistration(Class<T> serviceClass, T service) {
        this.serviceClass = serviceClass;
        this.service = service;
        this.serviceProvider = null;
    }

    /**
     * Create a service registration where the service will be initialized on first-use.
     *
     * @param serviceProvider the service provider
     */
    public ServiceRegistration(ServiceProvider<T> serviceProvider) {
        this.serviceClass = serviceProvider.getServiceClass();
        this.serviceProvider = serviceProvider;
    }

    /**
     * Get the service class type
     *
     * @return the class type, never {@code null}
     */
    public Class<T> getServiceClass() {
        return serviceClass;
    }

    /**
     * Get the service provider
     *
     * @return the service provider, may be {@code null}
     */
    public ServiceProvider<T> getServiceProvider() {
        return serviceProvider;
    }

    /**
     * Get the service instance.
     *
     * @return the service instance, may be {@code null} if initialized
     */
    public T getService() {
        return service;
    }

    /**
     * Set the service once initialized, used by {@link ServiceProvider} registrations.
     *
     * @param service the service instance, should not be {@code null}
     */
    public void setService(T service) {
        this.service = service;
    }

}
