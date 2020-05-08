/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.discovery.registry.nacos;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;

import io.seata.common.util.StringUtils;
import io.seata.config.Configuration;
import io.seata.config.ConfigurationFactory;
import io.seata.config.ConfigurationKeys;
import io.seata.discovery.registry.RegistryService;

/**
 * The type Nacos registry service.
 *
 * @author slievrly
 */
public class NacosRegistryServiceImpl implements RegistryService<EventListener> {
    private static final String DEFAULT_NAMESPACE = "";
    private static final String DEFAULT_CLUSTER = "default";
    private static final String DEFAULT_APPLICATION = "seata-server";
    private static final String PRO_SERVER_ADDR_KEY = "serverAddr";
    private static final String PRO_NAMESPACE_KEY = "namespace";
    private static final String REGISTRY_TYPE = "nacos";
    private static final String REGISTRY_CLUSTER = "cluster";
    private static final String PRO_APPLICATION_KEY = "application";
    private static final String USER_NAME = "username";
    private static final String PASSWORD = "password";
    private static final Configuration FILE_CONFIG = ConfigurationFactory.CURRENT_FILE_INSTANCE;
    private static volatile NamingService naming;
    private static final ConcurrentMap<String, List<EventListener>> LISTENER_SERVICE_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, List<InetSocketAddress>> CLUSTER_ADDRESS_MAP = new ConcurrentHashMap<>();
    private static volatile NacosRegistryServiceImpl instance;
    private static final Object LOCK_OBJ = new Object();

    private NacosRegistryServiceImpl() {
    }

    /**
     * Gets instance.
     *
     * @return the instance
     */
    static NacosRegistryServiceImpl getInstance() {
        if (null == instance) {
            synchronized (NacosRegistryServiceImpl.class) {
                if (null == instance) {
                    instance = new NacosRegistryServiceImpl();
                }
            }
        }
        return instance;
    }

    /**
     * 注册节点
     * @param address the address
     */
    @Override
    public void register(InetSocketAddress address) throws Exception {
        validAddress(address);
        getNamingInstance()
            .registerInstance(getServiceName(), address.getAddress().getHostAddress(), address.getPort(), getClusterName());
    }

    /**
     * 取消注册节点
     * @param address the address
     * @throws Exception
     */
    @Override
    public void unregister(InetSocketAddress address) throws Exception {
        validAddress(address);
        getNamingInstance()
            .deregisterInstance(getServiceName(), address.getAddress().getHostAddress(), address.getPort(), getClusterName());
    }

    /**
     * 添加监听器
     * @param cluster  the cluster
     * @param listener the listener
     * @throws Exception
     */
    @Override
    public void subscribe(String cluster, EventListener listener) throws Exception {
        List<String> clusters = new ArrayList<>();
        clusters.add(cluster);
        // 缓存
        LISTENER_SERVICE_MAP.putIfAbsent(cluster, new ArrayList<>());
        LISTENER_SERVICE_MAP.get(cluster).add(listener);
        // 订阅
        getNamingInstance().subscribe(getServiceName(), clusters, listener);
    }

    /**
     * 去除监听器
     */
    @Override
    public void unsubscribe(String cluster, EventListener listener) throws Exception {
        List<String> clusters = new ArrayList<>();
        clusters.add(cluster);
        // 缓存的监听列表
        List<EventListener> subscribeList = LISTENER_SERVICE_MAP.get(cluster);
        if (null != subscribeList) {
            // 过滤该集群下非该监听器的其他监听器
            List<EventListener> newSubscribeList = subscribeList.stream()
                    .filter(eventListener -> !eventListener.equals(listener))
                    .collect(Collectors.toList());
            // 覆盖...
            LISTENER_SERVICE_MAP.put(cluster, newSubscribeList);
        }
        // 取消订阅
        getNamingInstance().unsubscribe(getServiceName(), clusters, listener);
    }

    /**
     * 获取服务地址列表
     */
    @Override
    public List<InetSocketAddress> lookup(String key) throws Exception {
        String clusterName = getServiceGroup(key);
        if (null == clusterName) {
            return null;
        }
        if (!LISTENER_SERVICE_MAP.containsKey(clusterName)) {
            // 监听器缓存集合中不存在, 添加监听器
            synchronized (LOCK_OBJ) {
                if (!LISTENER_SERVICE_MAP.containsKey(clusterName)) {
                    List<String> clusters = new ArrayList<>();
                    clusters.add(clusterName);
                    // 所有实例
                    List<Instance> firstAllInstances = getNamingInstance().getAllInstances(getServiceName(), clusters);
                    if (null != firstAllInstances) {
                        // 过滤筛选出 开启 && 健康的实例列表
                        List<InetSocketAddress> newAddressList = firstAllInstances.stream()
                                .filter(instance -> instance.isEnabled() && instance.isHealthy())
                                .map(instance -> new InetSocketAddress(instance.getIp(), instance.getPort()))
                                .collect(Collectors.toList());
                        // 缓存
                        CLUSTER_ADDRESS_MAP.put(clusterName, newAddressList);
                    }
                    // 添加订阅者, 监听实例改变事件
                    subscribe(clusterName, event -> {
                        // 改变的实例
                        List<Instance> instances = ((NamingEvent)event).getInstances();
                        if (null == instances && null != CLUSTER_ADDRESS_MAP.get(clusterName)) {
                            // 不包含, 则清除集群对应的缓存
                            CLUSTER_ADDRESS_MAP.remove(clusterName);
                        } else if (!CollectionUtils.isEmpty(instances)) {
                            // 过滤筛选, 缓存
                            List<InetSocketAddress> newAddressList = instances.stream()
                                    .filter(instance -> instance.isEnabled() && instance.isHealthy())
                                    .map(instance -> new InetSocketAddress(instance.getIp(), instance.getPort()))
                                    .collect(Collectors.toList());
                            CLUSTER_ADDRESS_MAP.put(clusterName, newAddressList);
                        }
                    });
                }
            }
        }
        // 缓存集合中获取
        return CLUSTER_ADDRESS_MAP.get(clusterName);
    }

    @Override
    public void close() throws Exception {

    }

    /**
     * 效验注册地址
     */
    private void validAddress(InetSocketAddress address) {
        if (null == address.getHostName() || 0 == address.getPort()) {
            throw new IllegalArgumentException("invalid address:" + address);
        }
    }

    /**
     * Gets naming instance.
     *
     * @return the naming instance
     * @throws Exception the exception
     */
    public static NamingService getNamingInstance() throws Exception {
        if (null == naming) {
            synchronized (NacosRegistryServiceImpl.class) {
                if (null == naming) {
                    naming = NacosFactory.createNamingService(getNamingProperties());
                }
            }
        }
        return naming;
    }

    private static Properties getNamingProperties() {
        Properties properties = new Properties();
        // server_addr
        if (null != System.getProperty(PRO_SERVER_ADDR_KEY)) {
            // 系统属性中存在, 从系统属性中获取
            properties.setProperty(PRO_SERVER_ADDR_KEY, System.getProperty(PRO_SERVER_ADDR_KEY));
        } else {
            // 从配置文件中获取
            String address = FILE_CONFIG.getConfig(getNacosAddrFileKey());
            if (null != address) {
                properties.setProperty(PRO_SERVER_ADDR_KEY, address);
            }
        }

        // 命名空间 namespace
        if (null != System.getProperty(PRO_NAMESPACE_KEY)) {
            properties.setProperty(PRO_NAMESPACE_KEY, System.getProperty(PRO_NAMESPACE_KEY));
        } else {
            String namespace = FILE_CONFIG.getConfig(getNacosNameSpaceFileKey());
            if (null == namespace) {
                namespace = DEFAULT_NAMESPACE;
            }
            properties.setProperty(PRO_NAMESPACE_KEY, namespace);
        }
        String userName = StringUtils.isNotBlank(System.getProperty(USER_NAME)) ? System.getProperty(USER_NAME)
            : FILE_CONFIG.getConfig(getNacosUserName());
        if (StringUtils.isNotBlank(userName)) {
            String password = StringUtils.isNotBlank(System.getProperty(PASSWORD)) ? System.getProperty(PASSWORD)
                : FILE_CONFIG.getConfig(getNacosPassword());
            if (StringUtils.isNotBlank(password)) {
                properties.setProperty(USER_NAME, userName);
                properties.setProperty(PASSWORD, password);
            }
        }
        return properties;
    }

    private static String getClusterName() {
        return FILE_CONFIG.getConfig(getNacosClusterFileKey(), DEFAULT_CLUSTER);
    }

    private static String getServiceName() {
        return FILE_CONFIG.getConfig(getNacosApplicationFileKey(), DEFAULT_APPLICATION);
    }

    /**
     * @return "registry.nacos.serverAddr"
     */
    private static String getNacosAddrFileKey() {
        return String.join(ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
            ConfigurationKeys.FILE_ROOT_REGISTRY, REGISTRY_TYPE, PRO_SERVER_ADDR_KEY);
    }

    /**
     * @return "registry.nacos.namespace"
     */
    private static String getNacosNameSpaceFileKey() {
        return String.join(ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
            ConfigurationKeys.FILE_ROOT_REGISTRY, REGISTRY_TYPE, PRO_NAMESPACE_KEY);
    }

    /**
     * @return "registry.nacos.cluster"
     */
    private static String getNacosClusterFileKey() {
        return String.join(ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
            ConfigurationKeys.FILE_ROOT_REGISTRY, REGISTRY_TYPE, REGISTRY_CLUSTER);
    }

    private static String getNacosApplicationFileKey() {
        return String.join(ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR, ConfigurationKeys.FILE_ROOT_REGISTRY, REGISTRY_TYPE, PRO_APPLICATION_KEY);
    }

    private static String getNacosUserName() {
        return String.join(ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR, ConfigurationKeys.FILE_ROOT_REGISTRY, REGISTRY_TYPE,
            USER_NAME);
    }

    private static String getNacosPassword() {
        return String.join(ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR, ConfigurationKeys.FILE_ROOT_REGISTRY, REGISTRY_TYPE,
            PASSWORD);
    }
}
