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
package io.seata.rm.datasource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import io.seata.common.util.StringUtils;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.exception.TransactionException;
import io.seata.core.exception.TransactionExceptionCode;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.BranchType;
import io.seata.rm.DefaultResourceManager;
import io.seata.rm.datasource.exec.LockConflictException;
import io.seata.rm.datasource.exec.LockRetryController;
import io.seata.rm.datasource.undo.SQLUndoLog;
import io.seata.rm.datasource.undo.UndoLogManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.seata.core.constants.DefaultValues.DEFAULT_CLIENT_LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT;
import static io.seata.core.constants.DefaultValues.DEFAULT_CLIENT_REPORT_RETRY_COUNT;
import static io.seata.core.constants.DefaultValues.DEFAULT_CLIENT_REPORT_SUCCESS_ENABLE;

/**
 * 数据源连接代理
 * The type Connection proxy.
 *
 * @author sharajava
 */
public class ConnectionProxy extends AbstractConnectionProxy {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProxy.class);

    private ConnectionContext context = new ConnectionContext();

    /**
     * 重试次数
     */
    private static final int REPORT_RETRY_COUNT = ConfigurationFactory.getInstance()
        .getInt(ConfigurationKeys.CLIENT_REPORT_RETRY_COUNT, DEFAULT_CLIENT_REPORT_RETRY_COUNT);

    /**
     * 是否开启事务成功汇报
     */
    public static final boolean IS_REPORT_SUCCESS_ENABLE = ConfigurationFactory.getInstance()
        .getBoolean(ConfigurationKeys.CLIENT_REPORT_SUCCESS_ENABLE, DEFAULT_CLIENT_REPORT_SUCCESS_ENABLE);

    /**
     * 锁重试策略
     */
    private final static LockRetryPolicy LOCK_RETRY_POLICY = new LockRetryPolicy();

    /**
     * Instantiates a new Connection proxy.
     *
     * @param dataSourceProxy  the data source proxy
     * @param targetConnection the target connection
     */
    public ConnectionProxy(DataSourceProxy dataSourceProxy, Connection targetConnection) {
        super(dataSourceProxy, targetConnection);
    }

    /**
     * Gets context.
     *
     * @return the context
     */
    public ConnectionContext getContext() {
        return context;
    }

    /**
     * Bind.
     *
     * @param xid the xid
     */
    public void bind(String xid) {
        context.bind(xid);
    }

    /**
     * set global lock requires flag
     *
     * @param isLock whether to lock
     */
    public void setGlobalLockRequire(boolean isLock) {
        context.setGlobalLockRequire(isLock);
    }

    /**
     * get global lock requires flag
     */
    public boolean isGlobalLockRequire() {
        return context.isGlobalLockRequire();
    }

    /**
     * Check lock.
     *
     * @param lockKeys the lockKeys
     * @throws SQLException the sql exception
     */
    public void checkLock(String lockKeys) throws SQLException {
        // 检查全局锁
        // Just check lock without requiring lock by now.
        try {
            boolean lockable = DefaultResourceManager.get()
                .lockQuery(BranchType.AT, getDataSourceProxy().getResourceId(), context.getXid(), lockKeys);
            if (!lockable) {
                throw new LockConflictException();
            }
        } catch (TransactionException e) {
            recognizeLockKeyConflictException(e, lockKeys);
        }
    }

    /**
     * Lock query.
     *
     * @param lockKeys the lock keys
     * @throws SQLException the sql exception
     */
    public boolean lockQuery(String lockKeys) throws SQLException {
        // Just check lock without requiring lock by now.
        boolean result = false;
        try {
            result = DefaultResourceManager.get()
                .lockQuery(BranchType.AT, getDataSourceProxy().getResourceId(), context.getXid(), lockKeys);
        } catch (TransactionException e) {
            // 识别异常, 准确抛出
            recognizeLockKeyConflictException(e, lockKeys);
        }
        return result;
    }

    private void recognizeLockKeyConflictException(TransactionException te) throws SQLException {
        recognizeLockKeyConflictException(te, null);
    }

    /**
     * 识别异常, 准确抛出
     * @param te
     * @param lockKeys
     * @throws SQLException
     */
    private void recognizeLockKeyConflictException(TransactionException te, String lockKeys) throws SQLException {
        if (te.getCode() == TransactionExceptionCode.LockKeyConflict) {
            // 锁key冲突
            StringBuilder reasonBuilder = new StringBuilder("get global lock fail, xid:" + context.getXid());
            if (StringUtils.isNotBlank(lockKeys)) {
                reasonBuilder.append(", lockKeys:").append(lockKeys);
            }
            throw new LockConflictException(reasonBuilder.toString());
        } else {
            // 抛SQL异常
            throw new SQLException(te);
        }

    }

    /**
     * append sqlUndoLog
     *
     * @param sqlUndoLog the sql undo log
     */
    public void appendUndoLog(SQLUndoLog sqlUndoLog) {
        context.appendUndoItem(sqlUndoLog);
    }

    /**
     * append lockKey
     *
     * @param lockKey the lock key
     */
    public void appendLockKey(String lockKey) {
        context.appendLockKey(lockKey);
    }

    @Override
    public void commit() throws SQLException {
        try {
            LOCK_RETRY_POLICY.execute(() -> {
                // 执行提交
                doCommit();
                return null;
            });
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private void doCommit() throws SQLException {
        if (context.inGlobalTransaction()) {
            // 全局事务
            processGlobalTransactionCommit();
        } else if (context.isGlobalLockRequire()) {
            // 全局锁
            processLocalCommitWithGlobalLocks();
        } else {
            // 原始数据源连接提交事务
            targetConnection.commit();
        }
    }

    private void processLocalCommitWithGlobalLocks() throws SQLException {
        // 检测全局锁状态
        checkLock(context.buildLockKeys());
        try {
            // 原始数据源连接提交事务
            targetConnection.commit();
        } catch (Throwable ex) {
            throw new SQLException(ex);
        }
        context.reset();
    }

    private void processGlobalTransactionCommit() throws SQLException {
        try {
            // 向TC注册Branch事务
            register();
        } catch (TransactionException e) {
            // 识别异常, 准确抛出
            recognizeLockKeyConflictException(e, context.buildLockKeys());
        }

        try {
            if (context.hasUndoLog()) {
                // 需要记录Undo log, 获取对应Undo log管理器, 记录Undo log
                UndoLogManagerFactory.getUndoLogManager(this.getDbType()).flushUndoLogs(this);
            }
            // 原始数据源连接提交事务
            targetConnection.commit();
        } catch (Throwable ex) {
            LOGGER.error("process connectionProxy commit error: {}", ex.getMessage(), ex);
            // 向TC报告Branch事务
            report(false);
            throw new SQLException(ex);
        }
        if (IS_REPORT_SUCCESS_ENABLE) {
            // 若开启事务成功汇报, 向TC报告Branch事务结果
            report(true);
        }
        context.reset(); // 连接重置(清理Seata事务相关信息)
    }

    /**
     * 向TC注册Branch事务
     * @throws TransactionException
     */
    private void register() throws TransactionException {
        // 注册Branch事务(返回Branch事务ID)
        Long branchId = DefaultResourceManager.get() // 向TC发送请求
            .branchRegister(BranchType.AT, getDataSourceProxy().getResourceId(),
                null, context.getXid(), null, context.buildLockKeys()
            );
        context.setBranchId(branchId);
    }

    @Override
    public void rollback() throws SQLException {
        // 原始数据源连接回滚事务
        targetConnection.rollback();
        if (context.inGlobalTransaction() && context.isBranchRegistered()) {
            // 向TC报告Branch事务
            report(false);
        }
        // 连接重置(清理Seata事务相关信息)
        context.reset();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (autoCommit && !getAutoCommit()) {
            // change autocommit from false to true, we should commit() first according to JDBC spec.
            doCommit();
        }
        targetConnection.setAutoCommit(autoCommit);
    }

    /**
     * 向TC报告Branch事务
     * @param commitDone 是否已提交事务
     * @throws SQLException
     */
    private void report(boolean commitDone) throws SQLException {
        // 重试次数
        int retry = REPORT_RETRY_COUNT;
        while (retry > 0) {
            try {
                // 向TC发送请求
                DefaultResourceManager.get()
                    .branchReport(BranchType.AT, context.getXid(), context.getBranchId(),
                        commitDone ? BranchStatus.PhaseOne_Done : BranchStatus.PhaseOne_Failed, null
                    );
                return;
            } catch (Throwable ex) {
                LOGGER.error("Failed to report [" + context.getBranchId() + "/" + context.getXid() + "] commit done ["
                    + commitDone + "] Retry Countdown: " + retry);
                retry--;

                if (retry == 0) {
                    // 重试失败, 等TM超时重试
                    throw new SQLException("Failed to report branch status " + commitDone, ex);
                }
            }
        }
    }

    /**
     * 锁重试策略
     */
    public static class LockRetryPolicy {

        /**
         * 冲突时, Branch锁重试回滚
         */
        protected static final boolean LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT =
            ConfigurationFactory.getInstance()
                .getBoolean(ConfigurationKeys.CLIENT_LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT,
                    DEFAULT_CLIENT_LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT
                );

        public <T> T execute(Callable<T> callable) throws Exception {
            if (LOCK_RETRY_POLICY_BRANCH_ROLLBACK_ON_CONFLICT) {
                return callable.call();
            } else {
                return doRetryOnLockConflict(callable);
            }
        }

        /**
         * 锁冲突时执行重试
         */
        protected <T> T doRetryOnLockConflict(Callable<T> callable) throws Exception {
            LockRetryController lockRetryController = new LockRetryController();
            while (true) {
                try {
                    return callable.call();
                } catch (LockConflictException lockConflict) {
                    onException(lockConflict);
                    lockRetryController.sleep(lockConflict);
                } catch (Exception e) {
                    onException(e);
                    throw e;
                }
            }
        }

        /**
         * Callback on exception in doLockRetryOnConflict.
         *
         * @param e invocation exception
         * @throws Exception error
         */
        protected void onException(Exception e) throws Exception {
        }
    }
}
