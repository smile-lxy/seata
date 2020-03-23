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
package io.seata.tm;

import java.util.concurrent.TimeoutException;

import io.seata.core.exception.TmTransactionException;
import io.seata.core.exception.TransactionException;
import io.seata.core.exception.TransactionExceptionCode;
import io.seata.core.model.GlobalStatus;
import io.seata.core.model.TransactionManager;
import io.seata.core.protocol.ResultCode;
import io.seata.core.protocol.transaction.AbstractTransactionRequest;
import io.seata.core.protocol.transaction.AbstractTransactionResponse;
import io.seata.core.protocol.transaction.GlobalBeginRequest;
import io.seata.core.protocol.transaction.GlobalBeginResponse;
import io.seata.core.protocol.transaction.GlobalCommitRequest;
import io.seata.core.protocol.transaction.GlobalCommitResponse;
import io.seata.core.protocol.transaction.GlobalReportRequest;
import io.seata.core.protocol.transaction.GlobalReportResponse;
import io.seata.core.protocol.transaction.GlobalRollbackRequest;
import io.seata.core.protocol.transaction.GlobalRollbackResponse;
import io.seata.core.protocol.transaction.GlobalStatusRequest;
import io.seata.core.protocol.transaction.GlobalStatusResponse;
import io.seata.core.rpc.netty.TmRpcClient;

/**
 * The type Default transaction manager.
 *
 * @author sharajava
 */
public class DefaultTransactionManager implements TransactionManager {

    @Override
    public String begin(String applicationId, String transactionServiceGroup, String name, int timeout)
        throws TransactionException {
        // 封装全局事务Begin
        GlobalBeginRequest request = new GlobalBeginRequest();
        request.setTransactionName(name);
        request.setTimeout(timeout);
        // 向TC发送请求开启全局事务(返回事务ID)
        GlobalBeginResponse response = (GlobalBeginResponse)syncCall(request);
        if (response.getResultCode() == ResultCode.Failed) {
            // 失败则抛异常
            throw new TmTransactionException(TransactionExceptionCode.BeginFailed, response.getMsg());
        }
        // 成功则返回全局事务ID
        return response.getXid();
    }

    @Override
    public GlobalStatus commit(String xid) throws TransactionException {
        // 封装全局事务Commit
        GlobalCommitRequest globalCommit = new GlobalCommitRequest();
        globalCommit.setXid(xid);
        // 向TC发送请求
        GlobalCommitResponse response = (GlobalCommitResponse)syncCall(globalCommit);
        return response.getGlobalStatus();
    }

    @Override
    public GlobalStatus rollback(String xid) throws TransactionException {
        // 封装全局事务Rollback
        GlobalRollbackRequest globalRollback = new GlobalRollbackRequest();
        globalRollback.setXid(xid);
        // 向TC发送请求
        GlobalRollbackResponse response = (GlobalRollbackResponse)syncCall(globalRollback);
        return response.getGlobalStatus();
    }

    @Override
    public GlobalStatus getStatus(String xid) throws TransactionException {
        // 封装全局事务Query Status
        GlobalStatusRequest queryGlobalStatus = new GlobalStatusRequest();
        queryGlobalStatus.setXid(xid);
        // 向TC发送请求
        GlobalStatusResponse response = (GlobalStatusResponse)syncCall(queryGlobalStatus);
        return response.getGlobalStatus();
    }

    @Override
    public GlobalStatus globalReport(String xid, GlobalStatus globalStatus) throws TransactionException {
        // 封装全局事务Report
        GlobalReportRequest globalReport = new GlobalReportRequest();
        globalReport.setXid(xid);
        globalReport.setGlobalStatus(globalStatus);
        // 向TC发送请求
        GlobalReportResponse response = (GlobalReportResponse) syncCall(globalReport);
        return response.getGlobalStatus();
    }

    /**
     * 通过RPC向TC发送请求
     */
    private AbstractTransactionResponse syncCall(AbstractTransactionRequest request) throws TransactionException {
        try {
            return (AbstractTransactionResponse)TmRpcClient.getInstance().sendMsgWithResponse(request);
        } catch (TimeoutException toe) {
            throw new TmTransactionException(TransactionExceptionCode.IO, "RPC timeout", toe);
        }
    }
}
