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
package io.seata.server.metrics;

import io.seata.core.event.GlobalTransactionEvent;
import java.util.List;

import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.metrics.exporter.Exporter;
import io.seata.metrics.exporter.ExporterFactory;
import io.seata.metrics.registry.Registry;
import io.seata.metrics.registry.RegistryFactory;
import io.seata.server.event.EventBusManager;

/**
 * Metrics manager for init
 *
 * @author zhengyangyong
 */
public class MetricsManager {
    private static class SingletonHolder {
        private static MetricsManager INSTANCE = new MetricsManager();
    }

    public static final MetricsManager get() {
        return MetricsManager.SingletonHolder.INSTANCE;
    }

    private Registry registry;

    public Registry getRegistry() {
        return registry;
    }

    public void init() {
        // 'metrics.enabled'
        boolean enabled = ConfigurationFactory.getInstance()
            .getBoolean(ConfigurationKeys.METRICS_PREFIX + ConfigurationKeys.METRICS_ENABLED, false);
        if (enabled) {
            // 允许指标管理
            registry = RegistryFactory.getInstance();
            if (registry != null) {
                // 指标输出提供者列表
                List<Exporter> exporters = ExporterFactory.getInstanceList();
                //only at least one metrics exporter implement had imported in pom then need register MetricsSubscriber
                if (exporters.size() != 0) {
                    // 设置注册工厂, 各具体提供者在输出指标等相关操作时, 会用到
                    exporters.forEach(exporter -> exporter.setRegistry(registry));

                    /**
                     * 注册指标订阅者, 到时各指标数据通过EventBus传送, 订阅者拿到后进行相关操作
                     * @see MetricsSubscriber#recordGlobalTransactionEventForMetrics(GlobalTransactionEvent)
                     */
                    EventBusManager.get().register(new MetricsSubscriber(registry));
                }
            }
        }
    }
}
