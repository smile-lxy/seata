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
package io.seata.integration.dubbo.alibaba;

import com.alibaba.dubbo.common.extension.Activate;
import com.alibaba.dubbo.rpc.Filter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcContext;
import com.alibaba.dubbo.rpc.RpcException;
import io.seata.common.util.StringUtils;
import io.seata.core.context.RootContext;
import io.seata.core.constants.DubboConstants;
import io.seata.core.model.BranchType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Transaction propagation filter.
 *
 * @author sharajava
 */
@Activate(group = {DubboConstants.PROVIDER, DubboConstants.CONSUMER}, order = 100)
public class AlibabaDubboTransactionPropagationFilter implements Filter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlibabaDubboTransactionPropagationFilter.class);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        if (!DubboConstants.ALIBABADUBBO) {
            // 非阿里Dubbo, 执行原始方法
            return invoker.invoke(invocation);
        }
        // Consumer 调用 Provider时有
        // 事务ID
        String xid = RootContext.getXID();
        String branchType = RootContext.getBranchType();

        // Provider 被 Consumer 调用时有
        String rpcXid = getRpcXid();
        String rpcBranchType = RpcContext.getContext().getAttachment(RootContext.KEY_BRANCH_TYPE);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("xid in RootContext[{}] xid in RpcContext[{}]", xid, rpcXid);
        }
        boolean bind = false;
        if (xid != null) {
            // RPC上下文设置参数
            RpcContext.getContext().setAttachment(RootContext.KEY_XID, xid);
            RpcContext.getContext().setAttachment(RootContext.KEY_BRANCH_TYPE, branchType);
        } else {
            if (rpcXid != null) {
                // 绑定RPC事务
                RootContext.bind(rpcXid);
                if (StringUtils.equals(BranchType.TCC.name(), rpcBranchType)) {
                    // 绑定RPC事务拦截类型
                    RootContext.bindBranchType(BranchType.TCC);
                }
                bind = true;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("bind xid [{}] branchType [{}] to RootContext", rpcXid, rpcBranchType != null ? rpcBranchType : "AT");
                }
            }
        }
        try {
            // 执行原始方法
            return invoker.invoke(invocation);
        } finally {
            if (bind) {
                String unbindXid = RootContext.unbind();
                String previousBranchType = RootContext.getBranchType();
                if (StringUtils.equals(BranchType.TCC.name(), previousBranchType)) {
                    RootContext.unbindBranchType();
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("unbind xid [{}] branchType [{}] from RootContext", unbindXid, previousBranchType != null ? previousBranchType : "AT");
                }
                if (!rpcXid.equalsIgnoreCase(unbindXid)) {
                    LOGGER.warn("xid in change during RPC from {} to {},branchType from {} to {}", rpcXid, unbindXid,
                            rpcBranchType != null ? rpcBranchType : "AT", previousBranchType != null ? previousBranchType : "AT");
                    if (unbindXid != null) {
                        RootContext.bind(unbindXid);
                        LOGGER.warn("bind xid [{}] back to RootContext", unbindXid);
                        if (StringUtils.equals(BranchType.TCC.name(), previousBranchType)) {
                            RootContext.bindBranchType(BranchType.TCC);
                            LOGGER.warn("bind branchType [{}] back to RootContext", previousBranchType != null ? previousBranchType : "AT");
                        }
                    }
                }
            }
        }
    }

    /**
     * get rpc xid
     * @return
     */
    private String getRpcXid() {
        String rpcXid = RpcContext.getContext().getAttachment(RootContext.KEY_XID);
        if (rpcXid == null) {
            rpcXid = RpcContext.getContext().getAttachment(RootContext.KEY_XID.toLowerCase());
        }
        return rpcXid;
    }

}
