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

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.netty.channel.Channel;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.CollectionUtils;
import io.seata.common.util.DurationUtil;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.event.EventBus;
import io.seata.core.event.GlobalTransactionEvent;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.GlobalStatus;
import io.seata.core.protocol.AbstractMessage;
import io.seata.core.protocol.AbstractResultMessage;
import io.seata.core.protocol.transaction.AbstractTransactionRequestToTC;
import io.seata.core.protocol.transaction.AbstractTransactionResponse;
import io.seata.core.protocol.transaction.BranchRegisterRequest;
import io.seata.core.protocol.transaction.BranchRegisterResponse;
import io.seata.core.protocol.transaction.BranchReportRequest;
import io.seata.core.protocol.transaction.BranchReportResponse;
import io.seata.core.protocol.transaction.GlobalBeginRequest;
import io.seata.core.protocol.transaction.GlobalBeginResponse;
import io.seata.core.protocol.transaction.GlobalCommitRequest;
import io.seata.core.protocol.transaction.GlobalCommitResponse;
import io.seata.core.protocol.transaction.GlobalLockQueryRequest;
import io.seata.core.protocol.transaction.GlobalLockQueryResponse;
import io.seata.core.protocol.transaction.GlobalReportRequest;
import io.seata.core.protocol.transaction.GlobalReportResponse;
import io.seata.core.protocol.transaction.GlobalRollbackRequest;
import io.seata.core.protocol.transaction.GlobalRollbackResponse;
import io.seata.core.protocol.transaction.GlobalStatusRequest;
import io.seata.core.protocol.transaction.GlobalStatusResponse;
import io.seata.core.protocol.transaction.UndoLogDeleteRequest;
import io.seata.core.rpc.ChannelManager;
import io.seata.core.rpc.Disposable;
import io.seata.core.rpc.RpcContext;
import io.seata.core.rpc.ServerMessageSender;
import io.seata.core.rpc.TransactionMessageHandler;
import io.seata.core.rpc.netty.RpcServer;
import io.seata.server.AbstractTCInboundHandler;
import io.seata.server.event.EventBusManager;
import io.seata.server.session.GlobalSession;
import io.seata.server.session.SessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Default coordinator.
 */
public class DefaultCoordinator extends AbstractTCInboundHandler implements TransactionMessageHandler, Disposable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCoordinator.class);

    private static final int TIMED_TASK_SHUTDOWN_MAX_WAIT_MILLS = 5000;

    /**
     * The constant COMMITTING_RETRY_PERIOD.
     */
    protected static final long COMMITTING_RETRY_PERIOD = CONFIG
        .getLong(ConfigurationKeys.COMMITING_RETRY_PERIOD, 1000L);

    /**
     * The constant ASYNC_COMMITTING_RETRY_PERIOD.
     */
    protected static final long ASYNC_COMMITTING_RETRY_PERIOD = CONFIG
        .getLong(ConfigurationKeys.ASYN_COMMITING_RETRY_PERIOD, 1000L);

    /**
     * The constant ROLLBACKING_RETRY_PERIOD.
     */
    protected static final long ROLLBACKING_RETRY_PERIOD = CONFIG
        .getLong(ConfigurationKeys.ROLLBACKING_RETRY_PERIOD, 1000L);

    /**
     * The constant TIMEOUT_RETRY_PERIOD.
     */
    protected static final long TIMEOUT_RETRY_PERIOD = CONFIG
        .getLong(ConfigurationKeys.TIMEOUT_RETRY_PERIOD, 1000L);

    /**
     * The Transaction undo log delete period.
     */
    protected static final long UNDO_LOG_DELETE_PERIOD = CONFIG.getLong(
        ConfigurationKeys.TRANSACTION_UNDO_LOG_DELETE_PERIOD, 24 * 60 * 60 * 1000);

    /**
     * The Transaction undo log delay delete period
     */
    protected static final long UNDO_LOG_DELAY_DELETE_PERIOD = 3 * 60 * 1000;

    private static final int ALWAYS_RETRY_BOUNDARY = 0;

    private static final Duration MAX_COMMIT_RETRY_TIMEOUT = ConfigurationFactory.getInstance().getDuration(
        ConfigurationKeys.MAX_COMMIT_RETRY_TIMEOUT, DurationUtil.DEFAULT_DURATION, 100);

    private static final Duration MAX_ROLLBACK_RETRY_TIMEOUT = ConfigurationFactory.getInstance().getDuration(
        ConfigurationKeys.MAX_ROLLBACK_RETRY_TIMEOUT, DurationUtil.DEFAULT_DURATION, 100);

    private static final boolean ROLLBACK_RETRY_TIMEOUT_UNLOCK_ENABLE = ConfigurationFactory.getInstance()
        .getBoolean(ConfigurationKeys.ROLLBACK_RETRY_TIMEOUT_UNLOCK_ENABLE, false);

    private ScheduledThreadPoolExecutor retryRollbacking = new ScheduledThreadPoolExecutor(1,
        new NamedThreadFactory("RetryRollbacking", 1));

    private ScheduledThreadPoolExecutor retryCommitting = new ScheduledThreadPoolExecutor(1,
        new NamedThreadFactory("RetryCommitting", 1));

    private ScheduledThreadPoolExecutor asyncCommitting = new ScheduledThreadPoolExecutor(1,
        new NamedThreadFactory("AsyncCommitting", 1));

    private ScheduledThreadPoolExecutor timeoutCheck = new ScheduledThreadPoolExecutor(1,
        new NamedThreadFactory("TxTimeoutCheck", 1));

    private ScheduledThreadPoolExecutor undoLogDelete = new ScheduledThreadPoolExecutor(1,
        new NamedThreadFactory("UndoLogDelete", 1));

    private ServerMessageSender messageSender;

    private DefaultCore core;

    private EventBus eventBus = EventBusManager.get();

    /**
     * Instantiates a new Default coordinator.
     *
     * @param messageSender the message sender
     */
    public DefaultCoordinator(ServerMessageSender messageSender) {
        this.messageSender = messageSender;
        this.core = new DefaultCore(messageSender);
    }

    @Override
    protected void doGlobalBegin(GlobalBeginRequest request, GlobalBeginResponse response, RpcContext rpcContext)
        throws TransactionException {
        // 开启全局事务(返回Global事务ID)
        response.setXid(core.begin(rpcContext.getApplicationId(), rpcContext.getTransactionServiceGroup(),
            request.getTransactionName(), request.getTimeout())
        );
    }

    @Override
    protected void doGlobalCommit(GlobalCommitRequest request, GlobalCommitResponse response, RpcContext rpcContext)
        throws TransactionException {
        // 提交全局事务
        response.setGlobalStatus(core.commit(request.getXid()));
    }

    @Override
    protected void doGlobalRollback(GlobalRollbackRequest request, GlobalRollbackResponse response,
                                    RpcContext rpcContext) throws TransactionException {
        response.setGlobalStatus(core.rollback(request.getXid()));
    }

    @Override
    protected void doGlobalStatus(GlobalStatusRequest request, GlobalStatusResponse response, RpcContext rpcContext)
        throws TransactionException {
        // 设置全局事务状态
        response.setGlobalStatus(core.getStatus(request.getXid()));
    }

    @Override
    protected void doGlobalReport(GlobalReportRequest request, GlobalReportResponse response, RpcContext rpcContext)
        throws TransactionException {
        // 总事务汇报
        response.setGlobalStatus(core.globalReport(request.getXid(), request.getGlobalStatus()));
    }

    @Override
    protected void doBranchRegister(BranchRegisterRequest request, BranchRegisterResponse response,
                                    RpcContext rpcContext) throws TransactionException {
        // Branch注册(返回Branch事务ID)
        response.setBranchId(
            core.branchRegister(request.getBranchType(), request.getResourceId(), rpcContext.getClientId(),
                request.getXid(), request.getApplicationData(), request.getLockKey())
        );
    }

    @Override
    protected void doBranchReport(BranchReportRequest request, BranchReportResponse response, RpcContext rpcContext)
        throws TransactionException {
        core.branchReport(request.getBranchType(), request.getXid(), request.getBranchId(), request.getStatus(),
            request.getApplicationData());
    }

    @Override
    protected void doLockCheck(GlobalLockQueryRequest request, GlobalLockQueryResponse response, RpcContext rpcContext)
        throws TransactionException {
        response.setLockable(
            // 查询锁
            core.lockQuery(request.getBranchType(), request.getResourceId(), request.getXid(), request.getLockKey())
        );
    }

    /**
     * 超时检测
     * Timeout check.
     *
     * @throws TransactionException the transaction exception
     */
    protected void timeoutCheck() throws TransactionException {
        // 获取所有事务
        Collection<GlobalSession> allSessions = SessionHolder.getRootSessionManager().allSessions();
        if (CollectionUtils.isEmpty(allSessions)) {
            return;
        }
        if (allSessions.size() > 0 && LOGGER.isDebugEnabled()) {
            LOGGER.debug("Transaction Timeout Check Begin: " + allSessions.size());
        }
        for (GlobalSession globalSession : allSessions) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(globalSession.getXid() + " " + globalSession.getStatus() + " "
                    + globalSession.getBeginTime() + " " + globalSession.getTimeout());
            }
            boolean shouldTimeout = SessionHolder.lockAndExecute(globalSession, () -> {
                if (globalSession.getStatus() != GlobalStatus.Begin || !globalSession.isTimeout()) {
                    // 不处于开始状态, 未超时
                    return false;
                }
                // 添加生命周期更改监听器, 在关闭时, 及时调用相应事件, 已备监听器作出响应处理
                globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
                globalSession.close();
                globalSession.changeStatus(GlobalStatus.TimeoutRollbacking);

                /**
                 * 通过EventBus, 通知指标处理器作出处理
                 * @see io.seata.server.metrics.MetricsSubscriber#recordGlobalTransactionEventForMetrics(GlobalTransactionEvent)
                 */
                eventBus.post(
                    new GlobalTransactionEvent(globalSession.getTransactionId(), GlobalTransactionEvent.ROLE_TC,
                        globalSession.getTransactionName(), globalSession.getBeginTime(), null,
                        globalSession.getStatus())
                );

                return true;
            });
            if (!shouldTimeout) {
                continue;
            }
            LOGGER.info("Global transaction[" + globalSession.getXid() + "] is timeout and will be rolled back.");

            // 添加生命周期更改监听器, 在超时检测后, 及时调用相应事件, 已备监听器作出响应处理
            globalSession.addSessionLifecycleListener(SessionHolder.getRetryRollbackingSessionManager());
            // 添加到回滚事务集合中
            SessionHolder.getRetryRollbackingSessionManager().addGlobalSession(globalSession);

        }
        if (allSessions.size() > 0 && LOGGER.isDebugEnabled()) {
            LOGGER.debug("Transaction Timeout Check End. ");
        }

    }

    /**
     * 重试回滚
     * Handle retry rollbacking.
     */
    protected void handleRetryRollbacking() {
        // 需要重试回滚的事务
        Collection<GlobalSession> rollbackingSessions = SessionHolder.getRetryRollbackingSessionManager().allSessions();
        if (CollectionUtils.isEmpty(rollbackingSessions)) {
            return;
        }
        long now = System.currentTimeMillis();
        for (GlobalSession rollbackingSession : rollbackingSessions) {
            try {
                //  回滚中 && 小于12S 防止重复回滚
                // prevent repeated rollback
                if (rollbackingSession.getStatus().equals(GlobalStatus.Rollbacking) && !rollbackingSession.isRollbackingDead()) {
                    continue;
                }
                if (isRetryTimeout(now, MAX_ROLLBACK_RETRY_TIMEOUT.toMillis(), rollbackingSession.getBeginTime())) {
                    // 重试超时
                    if (ROLLBACK_RETRY_TIMEOUT_UNLOCK_ENABLE) {
                        // 回滚重试时不需开启事务锁, 释放事务锁
                        rollbackingSession.clean();
                    }
                    /**
                     * 移除全局事务记录
                     * Prevent thread safety issues
                     */
                    SessionHolder.getRetryRollbackingSessionManager().removeGlobalSession(rollbackingSession);
                    LOGGER.error("GlobalSession rollback retry timeout and removed [{}]", rollbackingSession.getXid());
                    continue;
                }
                // 添加生命周期更改监听器, 在重试回滚前后, 及时调用相应事件, 已备监听器作出响应处理
                rollbackingSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
                // 执行事务回滚
                core.doGlobalRollback(rollbackingSession, true);
            } catch (TransactionException ex) {
                LOGGER.info("Failed to retry rollbacking [{}] {} {}", rollbackingSession.getXid(), ex.getCode(),
                    ex.getMessage());
            }
        }
    }

    /**
     * Handle retry committing.
     */
    protected void handleRetryCommitting() {
        // 需要重试提交的事务
        Collection<GlobalSession> committingSessions = SessionHolder.getRetryCommittingSessionManager().allSessions();
        if (CollectionUtils.isEmpty(committingSessions)) {
            return;
        }
        long now = System.currentTimeMillis();
        for (GlobalSession committingSession : committingSessions) {
            try {
                if (isRetryTimeout(now, MAX_COMMIT_RETRY_TIMEOUT.toMillis(), committingSession.getBeginTime())) {
                    // 重试超时
                    /**
                     * Prevent thread safety issues
                     */
                    SessionHolder.getRetryCommittingSessionManager().removeGlobalSession(committingSession);
                    LOGGER.error("GlobalSession commit retry timeout and removed [{}]", committingSession.getXid());
                    continue;
                }
                // 添加生命周期更改监听器, 在重试提交前后, 及时调用相应事件, 已备监听器作出响应处理
                committingSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
                // 执行事务提交
                core.doGlobalCommit(committingSession, true);
            } catch (TransactionException ex) {
                LOGGER.info("Failed to retry committing [{}] {} {}", committingSession.getXid(), ex.getCode(),
                    ex.getMessage());
            }
        }
    }

    private boolean isRetryTimeout(long now, long timeout, long beginTime) {
        /**
         * Start timing when the session begin
         */
        if (timeout >= ALWAYS_RETRY_BOUNDARY && now - beginTime > timeout) {
            return true;
        }
        return false;
    }

    /**
     * Handle async committing.
     */
    protected void handleAsyncCommitting() {
        // 需要异步提交的事务
        Collection<GlobalSession> asyncCommittingSessions = SessionHolder.getAsyncCommittingSessionManager()
            .allSessions();
        if (CollectionUtils.isEmpty(asyncCommittingSessions)) {
            return;
        }
        for (GlobalSession asyncCommittingSession : asyncCommittingSessions) {
            try {
                // Instruction reordering in DefaultCore#asyncCommit may cause this situation
                if (GlobalStatus.AsyncCommitting != asyncCommittingSession.getStatus()) {
                    continue;
                }
                // 添加生命周期更改监听器, 在异步提交前后, 及时调用相应事件, 已备监听器作出响应处理
                asyncCommittingSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
                // 执行事务提交
                core.doGlobalCommit(asyncCommittingSession, true);
            } catch (TransactionException ex) {
                LOGGER.error("Failed to async committing [{}] {} {}", asyncCommittingSession.getXid(), ex.getCode(),
                    ex.getMessage(), ex);
            }
        }
    }

    /**
     * 清除undo日志
     * Undo log delete.
     */
    protected void undoLogDelete() {
        Map<String, Channel> rmChannels = ChannelManager.getRmChannels();
        if (rmChannels == null || rmChannels.isEmpty()) {
            LOGGER.info("no active rm channels to delete undo log");
            return;
        }
        // 配置的日志保存天数
        short saveDays = CONFIG.getShort(ConfigurationKeys.TRANSACTION_UNDO_LOG_SAVE_DAYS,
            UndoLogDeleteRequest.DEFAULT_SAVE_DAYS);
        for (Map.Entry<String, Channel> channelEntry : rmChannels.entrySet()) {
            String resourceId = channelEntry.getKey();
            // 封装参数
            UndoLogDeleteRequest deleteRequest = new UndoLogDeleteRequest();
            deleteRequest.setResourceId(resourceId);
            deleteRequest.setSaveDays(saveDays > 0 ? saveDays : UndoLogDeleteRequest.DEFAULT_SAVE_DAYS);
            try {
                // 向各RM发送清除undo日志消息
                messageSender.sendASyncRequest(channelEntry.getValue(), deleteRequest);
            } catch (Exception e) {
                LOGGER.error("Failed to async delete undo log resourceId = " + resourceId);
            }
        }
    }

    /**
     * Init.
     */
    public void init() {
        // 异常重试回滚任务
        retryRollbacking.scheduleAtFixedRate(() -> {
            try {
                handleRetryRollbacking();
            } catch (Exception e) {
                LOGGER.info("Exception retry rollbacking ... ", e);
            } // 1000
        }, 0, ROLLBACKING_RETRY_PERIOD, TimeUnit.MILLISECONDS);

        // 重试提交任务
        retryCommitting.scheduleAtFixedRate(() -> {
            try {
                handleRetryCommitting();
            } catch (Exception e) {
                LOGGER.info("Exception retry committing ... ", e);
            } // 1000
        }, 0, COMMITTING_RETRY_PERIOD, TimeUnit.MILLISECONDS);

        // 异步提交任务
        asyncCommitting.scheduleAtFixedRate(() -> {
            try {
                handleAsyncCommitting();
            } catch (Exception e) {
                LOGGER.info("Exception async committing ... ", e);
            } // 1000
        }, 0, ASYNC_COMMITTING_RETRY_PERIOD, TimeUnit.MILLISECONDS);

        // 超时检测任务
        timeoutCheck.scheduleAtFixedRate(() -> {
            try {
                timeoutCheck();
            } catch (Exception e) {
                LOGGER.info("Exception timeout checking ... ", e);
            } // 1000
        }, 0, TIMEOUT_RETRY_PERIOD, TimeUnit.MILLISECONDS);

        // undoLog清除任务
        undoLogDelete.scheduleAtFixedRate(() -> {
            try {
                undoLogDelete();
            } catch (Exception e) {
                LOGGER.info("Exception undoLog deleting ... ", e);
            } // 3min,                    24hour
        }, UNDO_LOG_DELAY_DELETE_PERIOD, UNDO_LOG_DELETE_PERIOD, TimeUnit.HOURS);
    }

    @Override
    public AbstractResultMessage onRequest(AbstractMessage request, RpcContext context) {
        if (!(request instanceof AbstractTransactionRequestToTC)) {
            throw new IllegalArgumentException();
        }
        // 强转为请求TC的Request
        AbstractTransactionRequestToTC transactionRequest = (AbstractTransactionRequestToTC) request;
        transactionRequest.setTCInboundHandler(this);

        // 委托模式(到具体实现执行)
        return transactionRequest.handle(context);
    }

    @Override
    public void onResponse(AbstractResultMessage response, RpcContext context) {
        if (!(response instanceof AbstractTransactionResponse)) {
            throw new IllegalArgumentException();
        }

    }

    @Override
    public void destroy() {
        // 1. first shutdown timed task 关闭定时任务线程池
        retryRollbacking.shutdown();
        retryCommitting.shutdown();
        asyncCommitting.shutdown();
        timeoutCheck.shutdown();
        try {
            retryRollbacking.awaitTermination(TIMED_TASK_SHUTDOWN_MAX_WAIT_MILLS, TimeUnit.MILLISECONDS);
            retryCommitting.awaitTermination(TIMED_TASK_SHUTDOWN_MAX_WAIT_MILLS, TimeUnit.MILLISECONDS);
            asyncCommitting.awaitTermination(TIMED_TASK_SHUTDOWN_MAX_WAIT_MILLS, TimeUnit.MILLISECONDS);
            timeoutCheck.awaitTermination(TIMED_TASK_SHUTDOWN_MAX_WAIT_MILLS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignore) {

        }
        // 2. second close netty flow 关闭服务RPC
        if (messageSender instanceof RpcServer) {
            ((RpcServer) messageSender).destroy();
        }
        // 3. last destroy SessionHolder 关闭事务管理器
        SessionHolder.destroy();
    }
}
