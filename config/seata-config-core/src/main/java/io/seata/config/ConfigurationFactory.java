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
package io.seata.config;

import java.util.Objects;

import io.seata.common.exception.NotSupportYetException;
import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.common.loader.EnhancedServiceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Configuration factory.
 *
 * @author slievrly
 * @author Geng Zhang
 */
public final class ConfigurationFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationFactory.class);

    private static final String REGISTRY_CONF_PREFIX = "registry";
    private static final String REGISTRY_CONF_SUFFIX = ".conf";
    private static final String ENV_SYSTEM_KEY = "SEATA_ENV";
    public static final String ENV_PROPERTY_KEY = "seataEnv";

    private static final String SYSTEM_PROPERTY_SEATA_CONFIG_NAME = "seata.config.name";

    private static final String ENV_SEATA_CONFIG_NAME = "SEATA_CONFIG_NAME";

    public static final Configuration CURRENT_FILE_INSTANCE;

    static {
        // 系统属性中获取'seata.config.name'
        String seataConfigName = System.getProperty(SYSTEM_PROPERTY_SEATA_CONFIG_NAME);
        if (null == seataConfigName) {
            // 环境变量中获取'SEATA_CONFIG_NAME'
            seataConfigName = System.getenv(ENV_SEATA_CONFIG_NAME);
        }
        if (null == seataConfigName) {
            // 默认: 'registry'
            seataConfigName = REGISTRY_CONF_PREFIX;
        }
        // 系统属性中获取'seataEnv'
        String envValue = System.getProperty(ENV_PROPERTY_KEY);
        if (null == envValue) {
            // 环境变量中获取'SEATA_ENV'
            envValue = System.getenv(ENV_SYSTEM_KEY);
        }
        Configuration configuration = (null == envValue)
            // registry.config
            ? new FileConfiguration(seataConfigName + REGISTRY_CONF_SUFFIX, false)
            // registry-${env}.config
            : new FileConfiguration(seataConfigName + "-" + envValue + REGISTRY_CONF_SUFFIX, false);
        Configuration extConfiguration = null;
        try {
            // SPI机制加载, 读取配置文件(目前默认: SpringBootConfigurationProvider)
            extConfiguration = EnhancedServiceLoader.load(ExtConfigurationProvider.class).provide(configuration);
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("load Configuration:{}", extConfiguration == null ? configuration.getClass().getSimpleName()
                    : extConfiguration.getClass().getSimpleName());
            }
        } catch (EnhancedServiceNotFoundException ignore) {

        } catch (Exception e) {
            LOGGER.error("failed to load extConfiguration:{}", e.getMessage(), e);
        }
        // 为空则用原始的Configuration, 有则用动态代理的Configuration
        CURRENT_FILE_INSTANCE = null == extConfiguration ? configuration : extConfiguration;
    }

    private static final String NAME_KEY = "name";
    private static final String FILE_TYPE = "file";

    private static volatile Configuration instance = null;

    /**
     * Gets instance.
     *
     * @return the instance
     */
    public static Configuration getInstance() {
        if (instance == null) {
            synchronized (Configuration.class) {
                if (instance == null) {
                    instance = buildConfiguration();
                }
            }
        }
        return instance;
    }

    private static Configuration buildConfiguration() {
        ConfigType configType;
        String configTypeName = null;
        try { // 'config.type'
            configTypeName = CURRENT_FILE_INSTANCE.getConfig(ConfigurationKeys.FILE_ROOT_CONFIG
                + ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR + ConfigurationKeys.FILE_ROOT_TYPE);
            configType = ConfigType.getType(configTypeName);
        } catch (Exception e) {
            throw new NotSupportYetException("not support register type: " + configTypeName, e);
        }
        if (ConfigType.File == configType) {
            // 配置文件存放是以File类型
            // 'config.file.name'
            String pathDataId = String.join(ConfigurationKeys.FILE_CONFIG_SPLIT_CHAR,
                ConfigurationKeys.FILE_ROOT_CONFIG, FILE_TYPE, NAME_KEY);
            // 配置文件名称
            String name = CURRENT_FILE_INSTANCE.getConfig(pathDataId);
            // 文件类型获取
            Configuration configuration = new FileConfiguration(name);
            Configuration extConfiguration = null;
            try {
                // 获取额外配置提供者, 读取配置文件
                extConfiguration = EnhancedServiceLoader.load(ExtConfigurationProvider.class).provide(configuration);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("load Configuration:{}",
                        extConfiguration == null ? configuration.getClass().getSimpleName()
                            : extConfiguration.getClass().getSimpleName());
                }
            } catch (EnhancedServiceNotFoundException ignore) {

            } catch (Exception e) {
                LOGGER.error("failed to load extConfiguration:{}", e.getMessage(), e);
            }
            return null == extConfiguration ? configuration : extConfiguration;
        } else {
            // SPI机制加载, 获取对应提供者做相应操作
            return EnhancedServiceLoader.load(ConfigurationProvider.class, Objects.requireNonNull(configType).name())
                .provide();
        }
    }
}
