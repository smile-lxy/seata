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
package io.seata.rm.tcc.interceptor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.seata.common.exception.FrameworkException;
import io.seata.common.util.StringUtils;
import io.seata.rm.tcc.api.BusinessActionContextParameter;

/**
 * Extracting TCC Context from Method
 *
 * @author zhangsen
 */
public class ActionContextUtil {

    private ActionContextUtil() {

    }

    /**
     * Extracting context data from parameters
     *
     * @param targetParam the target param
     * @return map map
     */
    public static Map<String, Object> fetchContextFromObject(Object targetParam) {
        try {
            Map<String, Object> context = new HashMap<>(8);
            List<Field> fields = new ArrayList<>();
            getAllField(targetParam.getClass(), fields);

            for (Field f : fields) {
                String fieldName = f.getName();
                BusinessActionContextParameter annotation = f.getAnnotation(BusinessActionContextParameter.class);

                if (annotation != null) {
                    f.setAccessible(true);
                    Object paramObject = f.get(targetParam);
                    int index = annotation.index();
                    if (annotation.isParamInProperty()) {
                        if (index >= 0) {
                            // 如果设置了指针, 按指针来
                            @SuppressWarnings("unchecked")
                            // 转换为List
                            Object targetObject = ((List<Object>) paramObject).get(index);
                            context.putAll(fetchContextFromObject(targetObject));
                        } else {
                            context.putAll(fetchContextFromObject(paramObject));
                        }
                    } else {
                        if (StringUtils.isBlank(annotation.paramName())) {
                            // 如果注解未指定字段名称, 默认按参数字段名称
                            context.put(fieldName, paramObject);
                        } else {
                            // 如果注解指定了字段名称, 按注解指定字段来
                            context.put(annotation.paramName(), paramObject);
                        }
                    }
                }
            }
            return context;
        } catch (Throwable t) {
            throw new FrameworkException(t, "fetchContextFromObject failover");
        }
    }

    /**
     * 获取对象及其上级所有字段
     * Gets all field.
     *
     * @param interFace the inter face
     * @param fields    the fields
     */
    public static void getAllField(Class<?> interFace, List<Field> fields) {
        if (interFace == Object.class || interFace.isInterface()) {
            return;
        }
        Field[] field = interFace.getDeclaredFields();
        fields.addAll(Arrays.asList(field));
        getAllField(interFace.getSuperclass(), fields);
    }

}
