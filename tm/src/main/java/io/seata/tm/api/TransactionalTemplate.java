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
package io.seata.tm.api;


import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.util.StringUtils;
import io.seata.core.context.RootContext;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.GlobalStatus;
import io.seata.tm.api.transaction.Propagation;
import io.seata.tm.api.transaction.SuspendedResourcesHolder;
import io.seata.tm.api.transaction.TransactionHook;
import io.seata.tm.api.transaction.TransactionHookManager;
import io.seata.tm.api.transaction.TransactionInfo;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Template of executing business logic with a global transaction.
 *
 * @author sharajava
 */
public class TransactionalTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalTemplate.class);


    /**
     * Execute object.
     *
     * @param business the business
     * @return the object
     * @throws TransactionalExecutor.ExecutionException the execution exception
     */
    public Object execute(TransactionalExecutor business) throws Throwable {
        // 1 get transactionInfo 事务信息
        TransactionInfo txInfo = business.getTransactionInfo();
        if (txInfo == null) {
            throw new ShouldNeverHappenException("transactionInfo does not exist");
        }
        // 全局事务
        // 1.1 get or create a transaction
        GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();

        // 事务传播等级
        // 1.2 Handle the Transaction propatation and the branchType
        Propagation propagation = txInfo.getPropagation();
        SuspendedResourcesHolder suspendedResourcesHolder = null;
        try {
            // 根据事务传播级别决定是否解绑或继承
            switch (propagation) {
                case NOT_SUPPORTED:
                    suspendedResourcesHolder = tx.suspend(true);
                    return business.execute();
                case REQUIRES_NEW:
                    suspendedResourcesHolder = tx.suspend(true);
                    break;
                case SUPPORTS:
                    if (!existingTransaction()) {
                        return business.execute();
                    }
                    break;
                case REQUIRED:
                    break;
                case NEVER:
                    if (existingTransaction()) {
                        throw new TransactionException(
                                String.format("Existing transaction found for transaction marked with propagation 'never',xid = %s"
                                        ,RootContext.getXID()));
                    } else {
                        return business.execute();
                    }
                case MANDATORY:
                    if (!existingTransaction()) {
                        throw new TransactionException("No existing transaction found for transaction marked with propagation 'mandatory'");
                    }
                    break;
                default:
                    throw new TransactionException("Not Supported Propagation:" + propagation);
            }

            try {
                // 开始事务
                // 2. begin transaction
                beginTransaction(txInfo, tx);

                Object rs = null;
                try {

                    // 执行业务
                    // Do Your Business
                    rs = business.execute();

                } catch (Throwable ex) {

                    // 根据异常决策是否回滚事务或提交事务
                    // 3.the needed business exception to rollback.
                    completeTransactionAfterThrowing(txInfo, tx, ex);
                    throw ex;
                }

                // 提交事务
                // 4. everything is fine, commit.
                commitTransaction(tx);

                return rs;
            } finally {
                //5. clear 触发事务完成钩子
                triggerAfterCompletion();
                cleanUp(); // 清除本地钩子
            }
        } finally {
            // 恢复事务
            tx.resume(suspendedResourcesHolder);
        }

    }

    public boolean existingTransaction() {
        return StringUtils.isNotEmpty(RootContext.getXID());

    }

    private void completeTransactionAfterThrowing(TransactionInfo txInfo, GlobalTransaction tx, Throwable ex) throws TransactionalExecutor.ExecutionException {
        //roll back
        if (txInfo != null && txInfo.rollbackOn(ex)) {
            // 回滚全局事务
            try {
                rollbackTransaction(tx, ex);
            } catch (TransactionException txe) {
                // Failed to rollback 执行回滚失败
                throw new TransactionalExecutor.ExecutionException(tx, txe,
                        TransactionalExecutor.Code.RollbackFailure, ex);
            }
        } else {
            // 不需要回滚事务, 继续提交事务
            // not roll back on this exception, so commit
            commitTransaction(tx);
        }
    }

    private void commitTransaction(GlobalTransaction tx) throws TransactionalExecutor.ExecutionException {
        try {
            // 触发事务提交前钩子
            triggerBeforeCommit();
            // 指定事务提交
            tx.commit();
            // 触发事务提交后钩子
            triggerAfterCommit();
        } catch (TransactionException txe) {
            // 4.1 Failed to commit
            throw new TransactionalExecutor.ExecutionException(tx, txe,
                TransactionalExecutor.Code.CommitFailure);
        }
    }

    private void rollbackTransaction(GlobalTransaction tx, Throwable ex) throws TransactionException, TransactionalExecutor.ExecutionException {
        // 触发事务回滚前钩子
        triggerBeforeRollback();
        // 执行事务回滚
        tx.rollback();
        // 触发事务回滚后钩子
        triggerAfterRollback();
        // 抛出事务异常
        // 3.1 Successfully rolled back
        throw new TransactionalExecutor.ExecutionException(tx, GlobalStatus.RollbackRetrying.equals(tx.getLocalStatus())
            ? TransactionalExecutor.Code.RollbackRetrying : TransactionalExecutor.Code.RollbackDone, ex);
    }

    /**
     * 开始事务
     */
    private void beginTransaction(TransactionInfo txInfo, GlobalTransaction tx) throws TransactionalExecutor.ExecutionException {
        try {
            // 触发事务开始前钩子
            triggerBeforeBegin();
            // 开启事务
            tx.begin(txInfo.getTimeOut(), txInfo.getName());
            // 触发事务开始后钩子
            triggerAfterBegin();
        } catch (TransactionException txe) {
            throw new TransactionalExecutor.ExecutionException(tx, txe,
                TransactionalExecutor.Code.BeginFailure);

        }
    }

    private void triggerBeforeBegin() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.beforeBegin();
            } catch (Exception e) {
                LOGGER.error("Failed execute beforeBegin in hook {}",e.getMessage(),e);
            }
        }
    }

    private void triggerAfterBegin() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.afterBegin();
            } catch (Exception e) {
                LOGGER.error("Failed execute afterBegin in hook {}",e.getMessage(),e);
            }
        }
    }

    private void triggerBeforeRollback() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.beforeRollback();
            } catch (Exception e) {
                LOGGER.error("Failed execute beforeRollback in hook {}",e.getMessage(),e);
            }
        }
    }

    private void triggerAfterRollback() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.afterRollback();
            } catch (Exception e) {
                LOGGER.error("Failed execute afterRollback in hook {}",e.getMessage(),e);
            }
        }
    }

    private void triggerBeforeCommit() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.beforeCommit();
            } catch (Exception e) {
                LOGGER.error("Failed execute beforeCommit in hook {}",e.getMessage(),e);
            }
        }
    }

    private void triggerAfterCommit() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.afterCommit();
            } catch (Exception e) {
                LOGGER.error("Failed execute afterCommit in hook {}",e.getMessage(),e);
            }
        }
    }

    private void triggerAfterCompletion() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.afterCompletion();
            } catch (Exception e) {
                LOGGER.error("Failed execute afterCompletion in hook {}",e.getMessage(),e);
            }
        }
    }

    private void cleanUp() {
        TransactionHookManager.clear();
    }

    private List<TransactionHook> getCurrentHooks() {
        return TransactionHookManager.getHooks();
    }

}
