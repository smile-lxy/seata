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
package io.seata.spring.boot.autoconfigure.provider;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import io.seata.common.holder.ObjectHolder;
import io.seata.config.Configuration;
import io.seata.config.ExtConfigurationProvider;
import org.apache.commons.lang.StringUtils;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;
import org.springframework.context.ApplicationContext;

import static io.seata.spring.boot.autoconfigure.StarterConstants.PROPERTY_MAP;
import static io.seata.spring.boot.autoconfigure.StarterConstants.SEATA_PREFIX;
import static io.seata.spring.boot.autoconfigure.StarterConstants.SERVICE_PREFIX;
import static io.seata.spring.boot.autoconfigure.StarterConstants.SPECIAL_KEY_GROUPLIST;
import static io.seata.spring.boot.autoconfigure.StarterConstants.SPECIAL_KEY_VGROUP_MAPPING;
import static io.seata.spring.boot.autoconfigure.util.StringFormatUtils.DOT;

/**
 * @author xingfudeshi@gmail.com
 */
public class SpringBootConfigurationProvider implements ExtConfigurationProvider {
    private static final String INTERCEPT_METHOD_PREFIX = "get"; // 拦截方法前缀

    @Override
    public Configuration provide(Configuration originalConfiguration) {
        // 动态代理创建拦截器
        return (Configuration) Enhancer.create(originalConfiguration.getClass(), new MethodInterceptor() {
            @Override
            public Object intercept(Object proxy, Method method, Object[] args, MethodProxy methodProxy)
                throws Throwable {
                // 方法名前缀为'get', 参数数量 > 0
                if (method.getName().startsWith(INTERCEPT_METHOD_PREFIX) && args.length > 0) {
                    Object result = null;
                    String rawDataId = (String) args[0];
                    if (args.length == 1) {
                        result = get(convertDataId(rawDataId));
                    } else if (args.length == 2) {
                        result = get(convertDataId(rawDataId), args[1]);
                    } else if (args.length == 3) {
                        result = get(convertDataId(rawDataId), args[1], (Long) args[2]); // 超时没用到...
                    }
                    if (null != result) {
                        // 如果返回类型要求String, 转换
                        //If the return type is String,need to convert the object to string
                        if (method.getReturnType().equals(String.class)) {
                            return String.valueOf(result);
                        }
                        return result;
                    }
                }

                // 反射获取
                return method.invoke(originalConfiguration, args);
            }
        });
    }

    private Object get(String dataId, Object defaultValue, long timeoutMills) throws IllegalAccessException {
        return get(dataId, defaultValue);

    }

    private Object get(String dataId, Object defaultValue) throws IllegalAccessException {
        Object result = get(dataId);
        if (null == result) {
            return defaultValue;
        }
        return result;
    }

    /**
     * 通过反射获取配置参数
     * @param dataId 参数key(例: seata.registry.redis.db)
     * @return
     * @throws IllegalAccessException
     */
    private Object get(String dataId) throws IllegalAccessException {
        String propertySuffix = getPropertySuffix(dataId); // 尾参数
        // 头参数对应属性实体类
        Class propertyClass = getPropertyClass(getPropertyPrefix(dataId));
        if (null != propertyClass) {
            // ApplicationContext获取对应属性实体类
            Object propertyObject = ObjectHolder.INSTANCE.getObject(ApplicationContext.class).getBean(propertyClass);
            Optional<Field> fieldOptional = Stream.of(propertyObject.getClass().getDeclaredFields())
                .filter(f -> f.getName().equalsIgnoreCase(propertySuffix))
                .findAny(); // 获取对应字段, 会被 provide()中的intercept()拦截, 通过反射找到返回
            if (fieldOptional.isPresent()) {
                Field field = fieldOptional.get();
                field.setAccessible(true);
                Object valueObject = field.get(propertyObject); // 获取对应的值
                if (valueObject instanceof Map) {
                    String key = StringUtils.substringAfterLast(dataId, String.valueOf(DOT)); // ${suffix}
                    valueObject = ((Map) valueObject).get(key);
                }
                return valueObject;
            }
        }
        return null;
    }

    /**
     * 转换数据
     * convert data id
     *
     * @param rawDataId
     * @return dataId
     */
    private String convertDataId(String rawDataId) {
        if (rawDataId.endsWith(SPECIAL_KEY_GROUPLIST)) {
            // 参数以'grouplist'结尾
            String suffix = StringUtils.removeEnd(rawDataId, DOT + SPECIAL_KEY_GROUPLIST); // 移除'.grouplist'
            //change the format of default.grouplist to grouplist.default
            return SERVICE_PREFIX + DOT + SPECIAL_KEY_GROUPLIST + DOT + suffix; // '.service.grouplist.${suffix}'
        }
        return SEATA_PREFIX + DOT + rawDataId; // 'seata.${dataId}'
    }

    /**
     * 获取头节点参数
     * Get property prefix
     *
     * @param dataId 例: seata.registry.redis.db
     * @return propertyPrefix 例: seata
     */
    private String getPropertyPrefix(String dataId) {
        if (dataId.contains(SPECIAL_KEY_VGROUP_MAPPING)) {
            return SERVICE_PREFIX;
        }
        if (dataId.contains(SPECIAL_KEY_GROUPLIST)) {
            return SERVICE_PREFIX;
        }
        return StringUtils.substringBeforeLast(dataId, String.valueOf(DOT));
    }

    /**
     * 获取尾节点参数
     * Get property suffix
     *
     * @param dataId 例: seata.registry.redis.db
     * @return propertySuffix 例: db
     */
    private String getPropertySuffix(String dataId) {
        if (dataId.contains(SPECIAL_KEY_VGROUP_MAPPING)) {
            // 包含'vgroupMapping'
            return SPECIAL_KEY_VGROUP_MAPPING; // 'vgroupMapping'
        }
        if (dataId.contains(SPECIAL_KEY_GROUPLIST)) {
            // 包含'grouplist'
            return SPECIAL_KEY_GROUPLIST; // 'grouplist'
        }
        return StringUtils.substringAfterLast(dataId, String.valueOf(DOT));
    }

    /**
     * 获取头参数对应实体类
     * Get property class
     * PROPERTY_MAP将在'StarterConstants'static{} 中初始化
     * @param propertyPrefix
     * @return propertyClass
     */
    private Class getPropertyClass(String propertyPrefix) {
        return PROPERTY_MAP.entrySet().stream()
            .filter(e -> propertyPrefix.equals(e.getKey()))
            .findAny()
            .map(Map.Entry::getValue)
            .orElse(null);
    }
}
