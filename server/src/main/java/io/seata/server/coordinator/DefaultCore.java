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
package io.seata.server.coordinator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.seata.common.exception.NotSupportYetException;
import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.common.util.CollectionUtils;
import io.seata.core.event.EventBus;
import io.seata.core.event.GlobalTransactionEvent;
import io.seata.core.exception.TransactionException;
import io.seata.core.logger.StackTraceLogger;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.BranchType;
import io.seata.core.model.GlobalStatus;
import io.seata.core.rpc.ServerMessageSender;
import io.seata.server.event.EventBusManager;
import io.seata.server.session.BranchSession;
import io.seata.server.session.GlobalSession;
import io.seata.server.session.SessionHelper;
import io.seata.server.session.SessionHolder;

/**
 * The type Default core.
 *
 * @author sharajava
 */
public class DefaultCore implements Core {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCore.class);

    private EventBus eventBus = EventBusManager.get();

    private static Map<BranchType, AbstractCore> coreMap = new ConcurrentHashMap<>();

    /**
     * get the Default core.
     *
     * @param messageSender the message sender
     */
    public DefaultCore(ServerMessageSender messageSender) {
        // SPI机制加载
        List<AbstractCore> allCore = EnhancedServiceLoader.loadAll(AbstractCore.class,
                new Class[] {ServerMessageSender.class}, new Object[] {messageSender});
        if (CollectionUtils.isNotEmpty(allCore)) {
            // 加载到缓存中, 方便策略者模式调用
            for (AbstractCore core : allCore) {
                coreMap.put(core.getHandleBranchType(), core);
            }
        }
    }

    /**
     * 获取对应事务处理器
     * get core
     *
     * @param branchType the branchType
     * @return the core
     */
    public AbstractCore getCore(BranchType branchType) {
        AbstractCore core = coreMap.get(branchType);
        if (core == null) {
            throw new NotSupportYetException("unsupported type:" + branchType.name());
        }
        return core;
    }

    /**
     * only for mock
     *
     * @param branchType the branchType
     * @param core the core
     */
    public void mockCore(BranchType branchType, AbstractCore core) {
        coreMap.put(branchType, core);
    }

    @Override
    public Long branchRegister(BranchType branchType, String resourceId, String clientId, String xid,
                               String applicationData, String lockKeys) throws TransactionException {
        // 获取相应处理器, 执行注册
        return getCore(branchType)
            .branchRegister(branchType, resourceId, clientId, xid, applicationData, lockKeys);
    }

    @Override
    public void branchReport(BranchType branchType, String xid, long branchId, BranchStatus status,
                             String applicationData) throws TransactionException {
        // 获取相应处理器, 执行汇报
        getCore(branchType).branchReport(branchType, xid, branchId, status, applicationData);
    }

    @Override
    public boolean lockQuery(BranchType branchType, String resourceId, String xid, String lockKeys)
            throws TransactionException {
        // 获取相应处理器, 执行查询
        return getCore(branchType).lockQuery(branchType, resourceId, xid, lockKeys);
    }

    @Override
    public BranchStatus branchCommit(GlobalSession globalSession, BranchSession branchSession) throws TransactionException {
        // 获取相应处理器, 执行提交
        return getCore(branchSession.getBranchType()).branchCommit(globalSession, branchSession);
    }

    @Override
    public BranchStatus branchRollback(GlobalSession globalSession, BranchSession branchSession) throws TransactionException {
        // 获取相应处理器, 执行回滚
        return getCore(branchSession.getBranchType()).branchRollback(globalSession, branchSession);
    }

    @Override
    public String begin(String applicationId, String transactionServiceGroup, String name, int timeout)
            throws TransactionException {
        // 封装全局事务
        GlobalSession session = GlobalSession.createGlobalSession(applicationId, transactionServiceGroup, name, timeout);
        // 添加生命周期更改监听器, 在开启全局事务时, 及时调用相应事件, 已备监听器作出响应处理
        session.addSessionLifecycleListener(SessionHolder.getRootSessionManager());

        // 执行开启全局事务
        session.begin();

        /** @see io.seata.server.metrics.MetricsSubscriber#recordGlobalTransactionEventForMetrics(GlobalTransactionEvent)  通过事件, 指标管理器进行处理 */
        // transaction start event
        eventBus.post(new GlobalTransactionEvent(session.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
                session.getTransactionName(), session.getBeginTime(), null, session.getStatus()));

        return session.getXid();
    }

    @Override
    public GlobalStatus commit(String xid) throws TransactionException {
        // 全局事务
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid);
        if (globalSession == null) {
            return GlobalStatus.Finished;
        }
        // 添加生命周期更改监听器, 在提交事务前后, 及时调用相应事件, 已备监听器作出响应处理
        globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
        // just lock changeStatus

        boolean shouldCommit = SessionHolder.lockAndExecute(globalSession, () -> {
            // the lock should release after branch commit
            // Highlight: Firstly, close the session, then no more branch can be registered.
            // 关闭全局事务激活状态, 释放Branch锁
            globalSession.closeAndClean();
            if (globalSession.getStatus() == GlobalStatus.Begin) {
                // 修改事务状态
                globalSession.changeStatus(GlobalStatus.Committing);
                return true;
            }
            return false;
        });
        if (!shouldCommit) { // 若不需要提交, 直接返回状态
            return globalSession.getStatus();
        }
        if (globalSession.canBeCommittedAsync()) {
            // 若允许异步提交, 执行异步提交, 返回状态
            globalSession.asyncCommit();
            return GlobalStatus.Committed;
        } else {
            // 执行同步提交
            doGlobalCommit(globalSession, false);
        }
        return globalSession.getStatus();
    }

    @Override
    public boolean doGlobalCommit(GlobalSession globalSession, boolean retrying) throws TransactionException {
        boolean success = true;
        /**
         * 通过EventBus, 通知指标处理器作出处理
         * @see io.seata.server.metrics.MetricsSubscriber#recordGlobalTransactionEventForMetrics(GlobalTransactionEvent)
         */
        eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
                globalSession.getTransactionName(), globalSession.getBeginTime(), null, globalSession.getStatus()));

        if (globalSession.isSaga()) {
            // 如果是Saga模式事务, 获取Saga事务处理器, 执行事务提交
            success = getCore(BranchType.SAGA).doGlobalCommit(globalSession, retrying);
        } else {
            for (BranchSession branchSession : globalSession.getSortedBranches()) {
                BranchStatus currentStatus = branchSession.getStatus();
                if (currentStatus == BranchStatus.PhaseOne_Failed) {
                    // 如果是一阶段失败的Branch, 从全局事务中移除
                    globalSession.removeBranch(branchSession);
                    continue;
                }
                try {
                    // 获取相应事务处理器, 执行事务提交
                    BranchStatus branchStatus = getCore(branchSession.getBranchType())
                        .branchCommit(globalSession, branchSession);

                    switch (branchStatus) {
                        case PhaseTwo_Committed:
                            // 二阶段已提交(成功), 从全局事务中移除
                            globalSession.removeBranch(branchSession);
                            continue;
                        case PhaseTwo_CommitFailed_Unretryable:
                            // 二阶段提交失败不可重试
                            if (globalSession.canBeCommittedAsync()) {
                                // 允许异步提交
                                LOGGER.error("Committing branch transaction[{}], status: PhaseTwo_CommitFailed_Unretryable, please check the business log.", branchSession.getBranchId());
                                continue;
                            } else {
                                // 结束提交失败
                                SessionHelper.endCommitFailed(globalSession);
                                LOGGER.error("Committing global transaction[{}] finally failed, caused by branch transaction[{}] commit failed.", globalSession.getXid(), branchSession.getBranchId());
                                return false;
                            }
                        default:
                            if (!retrying) {
                                // 不是重试中的事务, 添加到重试队列中, 重试提交
                                globalSession.queueToRetryCommit();
                                return false;
                            }
                            if (globalSession.canBeCommittedAsync()) {
                                // 允许异步提交
                                LOGGER.error("Committing branch transaction[{}], status:{} and will retry later",
                                    branchSession.getBranchId(), branchStatus);
                                continue;
                            } else {
                                LOGGER.error("Committing global transaction[{}] failed, caused by branch transaction[{}] commit failed, will retry later.", globalSession.getXid(), branchSession.getBranchId());
                                return false;
                            }
                    }
                } catch (Exception ex) {
                    StackTraceLogger.error(LOGGER, ex, "Committing branch transaction exception: {}",
                        new String[] {branchSession.toString()});
                    if (!retrying) {
                        // 不是重试中的事务, 添加到重试队列中, 重试提交
                        globalSession.queueToRetryCommit();
                        throw new TransactionException(ex);
                    }
                }
            }
            if (globalSession.hasBranch()) {
                // 全局事务里还存在Branch事务
                LOGGER.info("Committing global transaction is NOT done, xid = {}.", globalSession.getXid());
                return false;
            }
        }
        if (success) {
            // 全局事务提交成功, 结束提交
            SessionHelper.endCommitted(globalSession);

            /**
             * 通过EventBus, 通知指标处理器作出处理
             * @see io.seata.server.metrics.MetricsSubscriber#recordGlobalTransactionEventForMetrics(GlobalTransactionEvent)
             */
            eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
                    globalSession.getTransactionName(), globalSession.getBeginTime(), System.currentTimeMillis(),
                    globalSession.getStatus()));

            LOGGER.info("Committing global transaction is successfully done, xid = {}.", globalSession.getXid());
        }
        return success;
    }

    @Override
    public GlobalStatus rollback(String xid) throws TransactionException {
        // 全局事务
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid);
        if (globalSession == null) {
            return GlobalStatus.Finished;
        }
        // 添加生命周期更改监听器, 在回滚事务前后, 及时调用相应事件, 已备监听器作出响应处理
        globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
        // just lock changeStatus 事务加锁, 关闭事务
        boolean shouldRollBack = SessionHolder.lockAndExecute(globalSession, () -> {
            // 关闭全局事务激活状态
            globalSession.close(); // Highlight: Firstly, close the session, then no more branch can be registered.
            if (globalSession.getStatus() == GlobalStatus.Begin) {
                globalSession.changeStatus(GlobalStatus.Rollbacking);
                return true;
            }
            return false;
        });
        if (!shouldRollBack) {
            // 不需要回滚时, 直接返回事务状态
            return globalSession.getStatus();
        }

        // 执行全局事务回滚
        doGlobalRollback(globalSession, false);
        return globalSession.getStatus();
    }

    @Override
    public boolean doGlobalRollback(GlobalSession globalSession, boolean retrying) throws TransactionException {
        boolean success = true;
        /**
         * 通过EventBus, 通知指标处理器作出处理
         * @see io.seata.server.metrics.MetricsSubscriber#recordGlobalTransactionEventForMetrics(GlobalTransactionEvent)
         */
        eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
                globalSession.getTransactionName(), globalSession.getBeginTime(), null, globalSession.getStatus()));

        if (globalSession.isSaga()) {
            // 如果是Saga类型事务, 获取Saga事务处理器, 执行事务回滚
            success = getCore(BranchType.SAGA).doGlobalRollback(globalSession, retrying);
        } else {
            for (BranchSession branchSession : globalSession.getReverseSortedBranches()) {
                BranchStatus currentBranchStatus = branchSession.getStatus();
                if (currentBranchStatus == BranchStatus.PhaseOne_Failed) {
                    globalSession.removeBranch(branchSession);
                    // 如果是一阶段失败的Branch, 从全局事务中移除
                    continue;
                }
                try {
                    // 执行事务回滚
                    BranchStatus branchStatus = branchRollback(globalSession, branchSession);
                    switch (branchStatus) {
                        case PhaseTwo_Rollbacked:
                            // 二阶段已回滚(成功), 从全局事务中移除
                            globalSession.removeBranch(branchSession);
                            LOGGER.info("Rollback branch transaction  successfully, xid = {} branchId = {}", globalSession.getXid(), branchSession.getBranchId());
                            continue;
                        case PhaseTwo_RollbackFailed_Unretryable:
                            // 二阶段回滚失败不可重试, 结束回滚
                            SessionHelper.endRollbackFailed(globalSession);
                            LOGGER.info("Rollback branch transaction fail and stop retry, xid = {} branchId = {}", globalSession.getXid(), branchSession.getBranchId());
                            return false;
                        default:
                            LOGGER.info("Rollback branch transaction fail and will retry, xid = {} branchId = {}", globalSession.getXid(), branchSession.getBranchId());
                            if (!retrying) {
                                // 不是重试中的事务, 添加到重试队列中, 重试回滚
                                globalSession.queueToRetryRollback();
                            }
                            return false;
                    }
                } catch (Exception ex) {
                    StackTraceLogger.error(LOGGER, ex,
                        "Rollback branch transaction exception, xid = {} branchId = {} exception = {}",
                        new String[] {globalSession.getXid(), String.valueOf(branchSession.getBranchId()), ex.getMessage()});
                    if (!retrying) {
                        // 不是重试中的事务, 添加到重试队列中, 重试回滚
                        globalSession.queueToRetryRollback();
                    }
                    throw new TransactionException(ex);
                }
            }

            // In db mode, there is a problem of inconsistent data in multiple copies, resulting in new branch
            // transaction registration when rolling back.
            // 1. New branch transaction and rollback branch transaction have no data association
            // 2. New branch transaction has data association with rollback branch transaction
            // The second query can solve the first problem, and if it is the second problem, it may cause a rollback
            // failure due to data changes.
            // 重新查询最新总事务信息, 如果还有事务, 输出日志.
            GlobalSession globalSessionTwice = SessionHolder.findGlobalSession(globalSession.getXid());
            if (globalSessionTwice != null && globalSessionTwice.hasBranch()) {
                // 全局事务里还存在Branch事务
                LOGGER.info("Rollbacking global transaction is NOT done, xid = {}.", globalSession.getXid());
                return false;
            }
        }
        if (success) {
            // 结束事务回滚(移除全局事务记录)
            SessionHelper.endRollbacked(globalSession);

            /**
             * 通过EventBus, 通知指标处理器作出处理
             * @see io.seata.server.metrics.MetricsSubscriber#recordGlobalTransactionEventForMetrics(GlobalTransactionEvent)
             */
            eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
                    globalSession.getTransactionName(), globalSession.getBeginTime(), System.currentTimeMillis(),
                    globalSession.getStatus()));

            LOGGER.info("Rollback global transaction successfully, xid = {}.", globalSession.getXid());
        }
        return success;
    }

    @Override
    public GlobalStatus getStatus(String xid) throws TransactionException {
        // 全局事务
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid, false);
        if (null == globalSession) {
            return GlobalStatus.Finished;
        } else {
            return globalSession.getStatus();
        }
    }

    @Override
    public GlobalStatus globalReport(String xid, GlobalStatus globalStatus) throws TransactionException {
        // 全局事务
        GlobalSession globalSession = SessionHolder.findGlobalSession(xid);
        if (globalSession == null) {
            return globalStatus;
        }
        // 添加生命周期更改监听器, 在事务汇报后, 及时调用相应事件, 及时调用相应事件
        globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
        // 执行总事务汇报
        doGlobalReport(globalSession, xid, globalStatus);
        return globalSession.getStatus();
    }

    @Override
    public void doGlobalReport(GlobalSession globalSession, String xid, GlobalStatus globalStatus) throws TransactionException {
        if (globalSession.isSaga()) {
            // Saga模式下, 获取相应处理器, 执行总事务汇报
            getCore(BranchType.SAGA).doGlobalReport(globalSession, xid, globalStatus);
        }
    }
}
