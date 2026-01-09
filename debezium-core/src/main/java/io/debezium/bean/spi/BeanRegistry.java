/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.bean.spi;

import java.util.Map;
import java.util.Set;

import io.debezium.common.annotation.Incubating;
import io.debezium.service.Service;

/**
 * Represents a bean registry used to lookup components by name and type.
 *
 * The {@link BeanRegistry} extends the {@link Service} contract, allowing it to be exposed
 * via the service registry to any service easily.
 *
 * @author Chris Cranford
 */
// BeanRegistry（Bean 注册表）是一个非常轻量级的对象管理容器
// BeanRegistry 的主要作用是提供一种按名称（Name）和类型（Type）查找对象的机制。
// 组件仓库：它存储了连接器运行过程中需要共享的各种 POJO、工具类或配置实例（在 Debezium 中统称为 "Bean"）。
// 服务与 Bean 的桥梁：由于 BeanRegistry 继承了 Service 接口，它可以作为一种特殊的服务被注册到 ServiceRegistry 中。这意味着任何 Debezium 服务都可以通过依赖注入获取到 BeanRegistry，进而从中查找非服务类的普通对象。
// 支持多租户/多实例：在某些复杂的 CDC 场景中，可能存在多个同类型的组件（例如多个自定义转换器），通过 BeanRegistry 的“按名查找”功能，可以精确区分和获取它们。
@Incubating
public interface BeanRegistry extends Service {
    /**
     * Finds all beans that are registered by the specified type.
     *
     * @param type the class type to lookup
     * @return set of all beans found, may be empty, never {@code null}
     */
    // 获取所有属于指定类型（或其子类）的 Bean
    <T> Set<T> findByType(Class<T> type);

    /**
     * Finds all beans that are registered with the specified type.
     *
     * @param type the class type to lookup
     * @return map of bean instances with their mapping register names, may be empty, never {@code null}
     */
    // 获取指定类型的所有 Bean，并保留它们的注册名称。
    <T> Map<String, T> findByTypeWithName(Class<T> type);

    /**
     * Lookup a specific bean by its name and type.
     *
     * @param name the bean name to find
     * @param type the bean type to find
     * @return the bean or {@code null} if the bean could not be found
     */
    // 根据名称和类型进行精确查找。
    <T> T lookupByName(String name, Class<T> type);

    /**
     * Adds a bean to the registry.
     *
     * @param name the bean name the instance should be registered with, should not be {@code null}
     * @param type the bean class type, should not be {@code null}
     * @param bean the bean instance, should not be {@code null}
     */
    // 向注册表中手动添加一个 Bean，并显式指定其类型。
    void add(String name, Class<?> type, Object bean);

    /**
     * Adds a bean to the registry, resolving the class type from the {@code bean} instance.
     *
     * @param name the bean name the instance should be registered with, should not be {@code null}
     * @param bean the bean instance, should not be {@code null}
     */
    // 向注册表中添加 Bean，类型自动推断。
    void add(String name, Object bean);

    /**
     * Remove a bean from the registry by name.
     *
     * @param name the bean name
     */
    // 根据名称从注册表中移除某个 Bean。
    void remove(String name);
}
