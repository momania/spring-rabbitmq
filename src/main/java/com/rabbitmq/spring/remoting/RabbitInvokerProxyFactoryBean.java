package com.rabbitmq.spring.remoting;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.util.ClassUtils;

public class RabbitInvokerProxyFactoryBean extends RabbitInvokerClientInterceptor
        implements FactoryBean, BeanClassLoaderAware {

    private Class serviceInterface;

    private ClassLoader beanClassLoader = ClassUtils.getDefaultClassLoader();

    private Object serviceProxy;


    public void setServiceInterface(Class serviceInterface) {
        if (serviceInterface == null || !serviceInterface.isInterface()) {
            throw new IllegalArgumentException("'serviceInterface' must be an interface");
        }
        this.serviceInterface = serviceInterface;
    }

    public void setBeanClassLoader(ClassLoader classLoader) {
        beanClassLoader = classLoader;
    }

    @Override
    public void afterPropertiesSet() throws InterruptedException {
        super.afterPropertiesSet();
        if (serviceInterface == null) {
            throw new IllegalArgumentException("Property 'serviceInterface' is required");
        }
        serviceProxy = new ProxyFactory(serviceInterface, this).getProxy(beanClassLoader);
    }


    public Object getObject() {
        return serviceProxy;
    }

    public Class getObjectType() {
        return serviceInterface;
    }

    public boolean isSingleton() {
        return true;
    }

}
