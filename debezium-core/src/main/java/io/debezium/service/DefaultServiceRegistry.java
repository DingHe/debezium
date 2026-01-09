/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.service;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.ThreadSafe;
import io.debezium.bean.spi.BeanRegistry;
import io.debezium.common.annotation.Incubating;
import io.debezium.config.Configuration;
import io.debezium.service.spi.Configurable;
import io.debezium.service.spi.InjectService;
import io.debezium.service.spi.ServiceProvider;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.service.spi.ServiceRegistryAware;
import io.debezium.service.spi.Startable;

/**
 * Default implementation of the {@link ServiceRegistry}.
 *
 * @author Chris Cranford
 */
// DefaultServiceRegistry 是 Debezium 内部组件管理系统的核心实现类。它不仅是一个服务容器，还是一个微型的依赖注入（DI）框架。
// 集中式容器：它像一个“仓库”，存储了 Debezium 运行所需的所有服务实例（如：读取器、配置器、Bean 注册表等）。
// 依赖注入控制中心：它能识别服务类上的注解（如 @InjectService），自动将一个服务注入到另一个服务中，解决了组件间手动关联的麻烦。
// 多阶段初始化：它负责将一个服务的启动分为“创建 -> 依赖注入 -> 配置 -> 启动”四个标准阶段
// 线程安全的单例管理：确保在整个连接器任务生命周期内，每种类型的服务只被初始化一次。
@Incubating
@ThreadSafe
public class DefaultServiceRegistry implements ServiceRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultServiceRegistry.class);
    // 注册表索引。
    // 存储了服务类型（Class）与对应的 ServiceRegistration（注册信息/图纸）的映射。
    private final ConcurrentMap<Class<?>, ServiceRegistration<?>> serviceRegistrations = new ConcurrentHashMap<>();
    // 单例缓存。
    // 存储已经完成初始化并准备好运行的服务实例。查询服务时先看这里。
    private final ConcurrentMap<Class<?>, Service> initializedServices = new ConcurrentHashMap<>();
    // 初始化轨迹。
    // 按顺序记录了已经启动的服务。在 close() 时，会按此列表反向关闭。
    private final List<ServiceRegistration<?>> registrations = new ArrayList<>();
    // 全局配置。
    // 在创建和配置各个子服务时，将原始配置传递给它们。
    private final Configuration configuration;

    /**
     * Creates the default service registry, which registers the {@link BeanRegistry} as a service.
     *
     * @param configuration the user configuration, should not be {@code null}
     * @param beanRegistry the bean registry instance, should not be {@code null}
     */
    // 初始化注册表
    // 默认将 BeanRegistry 作为一个基础服务注册进去，因为很多其他服务需要用到它。
    public DefaultServiceRegistry(Configuration configuration, BeanRegistry beanRegistry) {
        this.configuration = configuration;
        registerService(new ServiceRegistration<>(BeanRegistry.class, beanRegistry), beanRegistry);
    }
    // 获取服务实例（最常用的方法）
    @Override
    public <T extends Service> T getService(Class<T> serviceClass) {
        T service = serviceClass.cast(initializedServices.get(serviceClass));
        if (service != null) {
            return service;
        }

        // Service initialization requires synchronization
        synchronized (this) {
            service = serviceClass.cast(initializedServices.get(serviceClass));
            if (service != null) {
                return service;
            }
            final ServiceRegistration<T> registration = findRegistration(serviceClass);
            if (registration == null) {
                throw new UnknownServiceException(serviceClass);
            }
            service = registration.getService();
            if (service == null) {
                service = initializeService(registration);
            }
            if (service != null) {
                initializedServices.put(serviceClass, service);
            }
            return service;
        }
    }

    @Override
    public void close() {
        initializedServices.clear();
        synchronized (registrations) {
            ListIterator<ServiceRegistration<?>> iterator = registrations.listIterator(registrations.size());
            while (iterator.hasPrevious()) {
                ServiceRegistration<?> registration = iterator.previous();
                try {
                    stopService(registration);
                }
                catch (IOException e) {
                    LOGGER.error("Failed to stop service " + registration.getServiceClass().getName(), e);
                }
            }
            registrations.clear();
        }
        serviceRegistrations.clear();
        LOGGER.info("Debezium ServiceRegistry stopped.");
    }
    // 向容器提交一份服务的“生产说明书”
    @Override
    public <T extends Service> void registerServiceProvider(ServiceProvider<T> serviceProvider) {
        final ServiceRegistration<T> registration = new ServiceRegistration<>(serviceProvider);
        serviceRegistrations.put(serviceProvider.getServiceClass(), registration);
    }
    //根据Provider创建服务
    private <T extends Service> T createService(ServiceProvider<T> serviceProvider) {
        return serviceProvider.createService(configuration, this);
    }
    // 配置服务
    private <T extends Service> void configureService(ServiceRegistration<T> registration) {
        if (registration.getService() instanceof Configurable) {
            ((Configurable) registration.getService()).configure(configuration);
        }
    }
    // 依赖注入
    private <T extends Service> void injectDependencies(ServiceRegistration<T> registration) {
        final T service = registration.getService();
        doInjections(service);

        if (service instanceof ServiceRegistryAware) {
            ((ServiceRegistryAware) service).injectServiceRegistry(this);
        }
    }
    // 启动服务
    private <T extends Service> void startService(ServiceRegistration<T> registration) {
        if (registration.getService() instanceof Startable) {
            ((Startable) registration.getService()).start();
        }
    }

    private <T extends Service> void stopService(ServiceRegistration<T> registration) throws IOException {
        if (registration.getService() instanceof Closeable) {
            ((Closeable) registration.getService()).close();
        }
    }
    // 注册服务
    private <T extends Service> void registerService(ServiceRegistration<T> registration, T service) {
        registration.setService(service);
        synchronized (registrations) {
            serviceRegistrations.put(registration.getServiceClass(), registration);
            registrations.add(registration);
        }
    }
    // Debezium 内部依赖注入（DI）机制的“扫描仪”。
    // 它的核心任务是利用 Java 反射技术，在服务启动阶段自动寻找并处理所有需要注入其他服务的 Setter 方法。
    // 专门负责为一个实现了 Service 接口的对象执行依赖注入。
    private <T extends Service> void doInjections(T service) {
        try {
            // 反射获取所有公有方法
            // Debezium 的注入是基于 Setter 方法 的。
            // 通过遍历方法列表，寻找那些符合条件的注入入口。
            for (Method method : service.getClass().getMethods()) {
                // 检查当前遍历到的方法上是否标记了 @InjectService 注解。
                InjectService injectService = method.getAnnotation(InjectService.class);
                if (injectService == null) {
                    continue;
                }
                // 调用更底层的 doInjection（注意是单数）方法来完成实际的注入操作。
                doInjection(service, method, injectService);
            }
        }
        catch (Exception e) {
            LOGGER.error("Failed to inject services into service: " + service.getClass().getName(), e);
        }
    }
    // Debezium 依赖注入机制的核心执行单元。
    // 它负责解析特定的注入需求，并利用 Java 反射将依赖服务正式“拨备”给目标服务。
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <T extends Service> void doInjection(T service, Method method, InjectService injectService) {
        // 检查被 @InjectService 标记的方法是否只有一个参数。
        final Class<?>[] parameterTypes = method.getParameterTypes();
        if (method.getParameterCount() != 1) {
            throw new ServiceDependencyException("InjectService on a method with an unexpected number of parameters");
        }

        // 推断到底需要注入哪种类型的服务
        // 显式指定：首先查看注解属性 injectService.service()。
        // 如果用户在注解里写了 @InjectService(service = MySpecificService.class)，则以此为准。
        Class requestedServiceType = injectService.service();
        if (requestedServiceType == null || requestedServiceType.equals(Void.class)) {
            // 隐式推断：如果注解里没写（默认是 Void.class），则自动使用该方法的参数类型作为请求的服务类型。这是最常用的模式。
            requestedServiceType = parameterTypes[0];
        }
        // 从注册中心查找服务实例
        final Service requestedService = getService(requestedServiceType);
        // 处理依赖不存在的情况。
        if (requestedService == null) {
            if (injectService.required()) {
                throw new ServiceDependencyException(String.format("Service '%s' not found, required by '%s'.",
                        requestedServiceType.getName(), service.getClass().getName()));
            }
        }
        else {
            // 通过反射调用方法
            // 真正将找到的服务实例“塞”进目标对象。
            try {
                method.invoke(service, requestedService);
            }
            catch (Exception e) {
                throw new ServiceDependencyException(String.format("Failed to inject service '%s' into '%s'.",
                        requestedServiceType.getName(), service.getClass().getName()));
            }
        }
    }
    // 查找注册信息
    @SuppressWarnings("unchecked")
    private <T extends Service> ServiceRegistration<T> findRegistration(Class<T> serviceClass) {
        return (ServiceRegistration<T>) serviceRegistrations.get(serviceClass);
    }
    // 启动服务的总开关
    private <T extends Service> T initializeService(ServiceRegistration<T> registration) {
        T service = createService(registration);
        if (service == null) {
            return null;
        }
        doMultiPhaseInitialization(registration);
        return service;
    }
    // 执行多阶段初始化
    // 顺序：1. 注入 (Inject) -> 2. 配置 (Configure) -> 3. 启动 (Start)。
    private <T extends Service> void doMultiPhaseInitialization(ServiceRegistration<T> registration) {
        injectDependencies(registration);
        configureService(registration);
        startService(registration);
    }
    // 创建并注册服务Service
    private <T extends Service> T createService(ServiceRegistration<T> registration) {
        final ServiceProvider<T> initiator = registration.getServiceProvider();
        if (initiator == null) {
            throw new UnknownServiceException(registration.getServiceClass());
        }
        try {
            T service = createService(initiator);
            // 注册新创建的服务
            if (service != null) {
                registerService(registration, service);
            }
            return service;
        }
        catch (Exception e) {
            throw new DebeziumException(String.format("Unable to create service %s",
                    registration.getServiceClass().getName()), e);
        }
    }

}
