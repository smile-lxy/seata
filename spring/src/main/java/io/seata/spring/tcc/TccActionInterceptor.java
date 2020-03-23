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
package io.seata.spring.tcc;

import io.seata.common.Constants;
import io.seata.core.context.RootContext;
import io.seata.core.model.BranchType;
import io.seata.rm.tcc.api.TwoPhaseBusinessAction;
import io.seata.rm.tcc.interceptor.ActionInterceptorHandler;
import io.seata.rm.tcc.remoting.RemotingDesc;
import io.seata.rm.tcc.remoting.parser.DubboUtil;
import io.seata.spring.util.SpringProxyUtils;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * TCC行为拦截
 * TCC Interceptor
 *
 * @author zhangsen
 */
public class TccActionInterceptor implements MethodInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TccActionInterceptor.class);

    private ActionInterceptorHandler actionInterceptorHandler = new ActionInterceptorHandler();

    /**
     * remoting bean info
     */
    protected RemotingDesc remotingDesc;

    /**
     * Instantiates a new Tcc action interceptor.
     */
    public TccActionInterceptor() {
    }

    /**
     * Instantiates a new Tcc action interceptor.
     *
     * @param remotingDesc the remoting desc
     */
    public TccActionInterceptor(RemotingDesc remotingDesc) {
        this.remotingDesc = remotingDesc;
    }

    @Override
    public Object invoke(final MethodInvocation invocation) throws Throwable {
        if (!RootContext.inGlobalTransaction()) {
            // 不在全局事务内, 执行原始方法
            //not in transaction
            return invocation.proceed();
        }
        // 接口方法
        Method method = getActionInterfaceMethod(invocation);
        // 事务注解
        TwoPhaseBusinessAction businessAction = method.getAnnotation(TwoPhaseBusinessAction.class);
        //try method
        if (businessAction != null) {
            //save the xid 事务ID
            String xid = RootContext.getXID();
            //clear the context 解绑事务
            RootContext.unbind();
            // 绑定接口层事务
            RootContext.bindInterceptorType(xid, BranchType.TCC);
            try {
                // 方法参数
                Object[] methodArgs = invocation.getArguments();
                //Handler the TCC Aspect 事务切面拦截
                Map<String, Object> ret = actionInterceptorHandler
                    .proceed(method, methodArgs, xid, businessAction, invocation::proceed);
                //return the final result
                return ret.get(Constants.TCC_METHOD_RESULT);
            } finally {
                // 解除接口层事务
                //recovery the context
                RootContext.unbindInterceptorType();
                // 绑定全局事务
                RootContext.bind(xid);
            }
        }
        // 没有事务, 执行原始方法
        return invocation.proceed();
    }

    /**
     * 从接口层获取方法(事务注解在接口层声明的)
     * get the method from interface
     *
     * @param invocation the invocation
     * @return the action interface method
     */
    protected Method getActionInterfaceMethod(MethodInvocation invocation) {
        try {
            // 接口类
            Class<?> interfaceType;
            if (remotingDesc == null) {
                interfaceType = getProxyInterface(invocation.getThis());
            } else {
                interfaceType = remotingDesc.getInterfaceClass();
            }
            if (interfaceType == null && remotingDesc.getInterfaceClassName() != null) {
                // 未找到 && 接口名不为空, 反射获取
                interfaceType = Class.forName(remotingDesc.getInterfaceClassName(), true,
                    Thread.currentThread().getContextClassLoader());
            }
            if (interfaceType == null) {
                // 接口类未空, 返回实现类方法
                return invocation.getMethod();
            }
            // 接口层抽象方法
            return interfaceType.getMethod(invocation.getMethod().getName(),
                invocation.getMethod().getParameterTypes());
        } catch (Exception e) {
            LOGGER.warn("get Method from interface failed", e);
            return invocation.getMethod();
        }
    }

    /**
     * get the interface of proxy
     *
     * @param proxyBean the proxy bean
     * @return proxy interface
     * @throws Exception the exception
     */
    protected Class<?> getProxyInterface(Object proxyBean) throws Exception {
        if (DubboUtil.isDubboProxyName(proxyBean.getClass().getName())) {
            // Dubbo结构, 用Dubbo的方法获取
            //dubbo javaassist proxy
            return DubboUtil.getAssistInterface(proxyBean);
        } else {
            //jdk/cglib proxy
            return SpringProxyUtils.getTargetInterface(proxyBean);
        }
    }
}
