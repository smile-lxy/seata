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
package io.seata.rm.datasource.undo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.seata.common.Constants;
import io.seata.common.util.CollectionUtils;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ClientTableColumnsName;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.exception.BranchTransactionException;
import io.seata.core.exception.TransactionException;
import io.seata.rm.datasource.ConnectionContext;
import io.seata.rm.datasource.ConnectionProxy;
import io.seata.rm.datasource.DataSourceProxy;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableMetaCacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.seata.core.constants.DefaultValues.DEFAULT_TRANSACTION_UNDO_LOG_TABLE;
import static io.seata.core.exception.TransactionExceptionCode.BranchRollbackFailed_Retriable;

/**
 * @author jsbxyyx
 */
public abstract class AbstractUndoLogManager implements UndoLogManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUndoLogManager.class);

    protected enum State {
        /**
         * 可正常回滚
         * This state can be properly rolled back by services
         */
        Normal(0),
        /**
         * 防悬挂
         * This state prevents the branch transaction from inserting undo_log
         * after the global transaction is rolled back.
         */
        GlobalFinished(1);

        private int value;

        State(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * undo_log
     */
    protected static final String UNDO_LOG_TABLE_NAME = ConfigurationFactory.getInstance()
        .getConfig(ConfigurationKeys.TRANSACTION_UNDO_LOG_TABLE, DEFAULT_TRANSACTION_UNDO_LOG_TABLE);

    /**
     * select * from undo_log where branch_id = ? and xid = ? for update
     */
    protected static final String SELECT_UNDO_LOG_SQL = "SELECT * FROM " + UNDO_LOG_TABLE_NAME + " WHERE "
        + ClientTableColumnsName.UNDO_LOG_BRANCH_XID + " = ? AND " + ClientTableColumnsName.UNDO_LOG_XID
        + " = ? FOR UPDATE";

    /**
     * delete from undo_log where barnch_id =? and xid = ?
     */
    protected static final String DELETE_UNDO_LOG_SQL = "DELETE FROM " + UNDO_LOG_TABLE_NAME + " WHERE "
        + ClientTableColumnsName.UNDO_LOG_BRANCH_XID + " = ? AND " + ClientTableColumnsName.UNDO_LOG_XID + " = ?";

    private static final ThreadLocal<String> SERIALIZER_LOCAL = new ThreadLocal<>();

    public static String getCurrentSerializer() {
        return SERIALIZER_LOCAL.get();
    }

    public static void setCurrentSerializer(String serializer) {
        SERIALIZER_LOCAL.set(serializer);
    }

    public static void removeCurrentSerializer() {
        SERIALIZER_LOCAL.remove();
    }

    /**
     * 移除undo日志
     * Delete undo log.
     *
     * @param xid      the xid
     * @param branchId the branch id
     * @param conn     the conn
     * @throws SQLException the sql exception
     */
    @Override
    public void deleteUndoLog(String xid, long branchId, Connection conn) throws SQLException {
        // 预编译
        try (PreparedStatement deletePST = conn.prepareStatement(DELETE_UNDO_LOG_SQL)) {
            // 填充SQL值
            deletePST.setLong(1, branchId);
            deletePST.setString(2, xid);
            // 执行
            deletePST.executeUpdate();
        } catch (Exception e) {
            if (!(e instanceof SQLException)) {
                e = new SQLException(e);
            }
            throw (SQLException) e;
        }
    }

    /**
     * batch Delete undo log.
     *
     * @param xids
     * @param branchIds
     * @param conn
     */
    @Override
    public void batchDeleteUndoLog(Set<String> xids, Set<Long> branchIds, Connection conn) throws SQLException {
        if (CollectionUtils.isEmpty(xids) || CollectionUtils.isEmpty(branchIds)) {
            return;
        }
        int xidSize = xids.size();
        int branchIdSize = branchIds.size();
        // SQL语句
        String batchDeleteSql = toBatchDeleteUndoLogSql(xidSize, branchIdSize);
        // 预编译
        try (PreparedStatement deletePST = conn.prepareStatement(batchDeleteSql)) {
            // 填充SQL值
            int paramsIndex = 1;
            for (Long branchId : branchIds) {
                deletePST.setLong(paramsIndex++, branchId);
            }
            for (String xid : xids) {
                deletePST.setString(paramsIndex++, xid);
            }
            // 执行
            int deleteRows = deletePST.executeUpdate();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("batch delete undo log size {}", deleteRows);
            }
        } catch (Exception e) {
            if (!(e instanceof SQLException)) {
                e = new SQLException(e);
            }
            throw (SQLException) e;
        }
    }

    /**
     * 批量删除Undo log的SQL
     * delete from undo_log where branch_id in ( ?, ?, ?, ...) and xid in ( ?, ?, ?, ...)
     */
    protected static String toBatchDeleteUndoLogSql(int xidSize, int branchIdSize) {
        StringBuilder sqlBuilder = new StringBuilder(64);
        sqlBuilder.append("DELETE FROM ")
            .append(UNDO_LOG_TABLE_NAME)
            .append(" WHERE  ")
            .append(ClientTableColumnsName.UNDO_LOG_BRANCH_XID)
            .append(" IN ");
        appendInParam(branchIdSize, sqlBuilder);
        sqlBuilder.append(" AND ")
            .append(ClientTableColumnsName.UNDO_LOG_XID)
            .append(" IN ");
        appendInParam(xidSize, sqlBuilder);
        return sqlBuilder.toString();
    }

    protected static void appendInParam(int size, StringBuilder sqlBuilder) {
        sqlBuilder.append(" (");
        for (int i = 0; i < size; i++) {
            sqlBuilder.append("?");
            if (i < (size - 1)) {
                sqlBuilder.append(",");
            }
        }
        sqlBuilder.append(") ");
    }

    protected static boolean canUndo(int state) {
        return state == State.Normal.getValue();
    }

    protected String buildContext(String serializer) {
        Map<String, String> map = new HashMap<>();
        map.put(UndoLogConstants.SERIALIZER_KEY, serializer);
        return CollectionUtils.encodeMap(map);
    }

    protected Map<String, String> parseContext(String data) {
        return CollectionUtils.decodeMap(data);
    }

    /**
     * Flush undo logs.
     *
     * @param cp the cp
     * @throws SQLException the sql exception
     */
    @Override
    public void flushUndoLogs(ConnectionProxy cp) throws SQLException {
        ConnectionContext connectionContext = cp.getContext();
        String xid = connectionContext.getXid();
        long branchId = connectionContext.getBranchId();

        BranchUndoLog branchUndoLog = new BranchUndoLog();
        branchUndoLog.setXid(xid);
        branchUndoLog.setBranchId(branchId);
        branchUndoLog.setSqlUndoLogs(connectionContext.getUndoItems());

        // Undo log 解析器
        UndoLogParser parser = UndoLogParserFactory.getInstance();
        byte[] undoLogContent = parser.encode(branchUndoLog); // 编码

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Flushing UNDO LOG: {}", new String(undoLogContent, Constants.DEFAULT_CHARSET));
        }

        insertUndoLogWithNormal(xid, branchId, buildContext(parser.getName()),
            undoLogContent, cp.getTargetConnection());
    }

    /**
     * Undo.
     *
     * @param dataSourceProxy the data source proxy
     * @param xid             the xid
     * @param branchId        the branch id
     * @throws TransactionException the transaction exception
     */
    @Override
    public void undo(DataSourceProxy dataSourceProxy, String xid, long branchId) throws TransactionException {
        Connection conn = null;
        ResultSet rs = null;
        PreparedStatement selectPST = null;
        boolean originalAutoCommit = true;

        for (; ; ) {
            try {
                // 原始数据源连接
                conn = dataSourceProxy.getPlainConnection();

                // 取消自动提交
                // The entire undo process should run in a local transaction.
                if (originalAutoCommit = conn.getAutoCommit()) {
                    conn.setAutoCommit(false);
                }

                // Find UNDO LOG 预编译
                selectPST = conn.prepareStatement(SELECT_UNDO_LOG_SQL);
                // 填充SQL值
                selectPST.setLong(1, branchId);
                selectPST.setString(2, xid);
                // 执行
                rs = selectPST.executeQuery();

                // 是否存在Undo log标识
                boolean exists = false;
                while (rs.next()) {
                    exists = true;

                    // 幂等处理
                    // It is possible that the server repeatedly sends a rollback request to roll back
                    // the same branch transaction to multiple processes,
                    // ensuring that only the undo_log in the normal state is processed.
                    int state = rs.getInt(ClientTableColumnsName.UNDO_LOG_LOG_STATUS);
                    if (!canUndo(state)) {
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("xid {} branch {}, ignore {} undo_log", xid, branchId, state);
                        }
                        return;
                    }

                    String contextString = rs.getString(ClientTableColumnsName.UNDO_LOG_CONTEXT);
                    // 解析上下文
                    Map<String, String> context = parseContext(contextString);
                    byte[] rollbackInfo = getRollbackInfo(rs); // 回滚信息

                    // 根据上下文中指定的序列化方式, 获取相关序列化处理器, 反序列化回滚信息
                    String serializer = context == null ? null : context.get(UndoLogConstants.SERIALIZER_KEY);
                    UndoLogParser parser = serializer == null
                        ? UndoLogParserFactory.getInstance()
                        : UndoLogParserFactory.getInstance(serializer);
                    // 反序列化后Branch Undo log
                    BranchUndoLog branchUndoLog = parser.decode(rollbackInfo);

                    try {
                        // put serializer name to local
                        setCurrentSerializer(parser.getName());
                        // 获取Branch下undo日志列表, 反转
                        List<SQLUndoLog> sqlUndoLogs = branchUndoLog.getSqlUndoLogs();
                        if (sqlUndoLogs.size() > 1) {
                            Collections.reverse(sqlUndoLogs);
                        }
                        for (SQLUndoLog sqlUndoLog : sqlUndoLogs) {
                            TableMeta tableMeta = TableMetaCacheFactory.getTableMetaCache(dataSourceProxy.getDbType())
                                .getTableMeta(conn, sqlUndoLog.getTableName(), dataSourceProxy.getResourceId());
                            sqlUndoLog.setTableMeta(tableMeta);
                            // 获取undo相应处理器
                            AbstractUndoExecutor undoExecutor = UndoExecutorFactory
                                .getUndoExecutor(dataSourceProxy.getDbType(), sqlUndoLog);
                            // 执行
                            undoExecutor.executeOn(conn);
                        }
                    } finally {
                        // remove serializer name 清除
                        removeCurrentSerializer();
                    }
                }

                // 防悬挂
                // If undo_log exists, it means that the branch transaction has completed the first phase,
                // we can directly roll back and clean the undo_log
                // Otherwise, it indicates that there is an exception in the branch transaction,
                // causing undo_log not to be written to the database.
                // For example, the business processing timeout, the global transaction is the initiator rolls back.
                // To ensure data consistency, we can insert an undo_log with GlobalFinished state
                // to prevent the local transaction of the first phase of other programs from being correctly submitted.
                // See https://github.com/seata/seata/issues/489

                if (exists) {
                    // 存在Undo log, 移除Undo日志
                    deleteUndoLog(xid, branchId, conn);
                    // 提交事务
                    conn.commit();
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("xid {} branch {}, undo_log deleted with {}", xid, branchId,
                            State.GlobalFinished.name());
                    }
                } else {
                    // 不存在Undo log, 添加一条Undo log 标识完成,
                    // 防悬挂(因各种原因, 第二阶段回滚比第一阶段早执行, 防止第一阶段执行导致破坏数据一致性, 添加一条Undo log)
                    insertUndoLogWithGlobalFinished(xid, branchId, UndoLogParserFactory.getInstance(), conn);
                    // 事务提交
                    conn.commit();
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("xid {} branch {}, undo_log added with {}", xid, branchId,
                            State.GlobalFinished.name());
                    }
                }

                return;
            } catch (SQLIntegrityConstraintViolationException e) {
                // 具有唯一约束条件的列的值有重复
                // Possible undo_log has been inserted into the database by other processes, retrying rollback undo_log
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("xid {} branch {}, undo_log inserted, retry rollback", xid, branchId);
                }
            } catch (Throwable e) {
                if (conn != null) {
                    try {
                        // 事务回滚
                        conn.rollback();
                    } catch (SQLException rollbackEx) {
                        LOGGER.warn("Failed to close JDBC resource while undo ... ", rollbackEx);
                    }
                }
                throw new BranchTransactionException(BranchRollbackFailed_Retriable,
                    String.format("Branch session rollback failed and try again later xid = %s branchId = %s %s",
                        xid, branchId, e.getMessage()), e);

            } finally {
                // 清理, 重置
                try {
                    if (rs != null) {
                        rs.close();
                    }
                    if (selectPST != null) {
                        selectPST.close();
                    }
                    if (conn != null) {
                        if (originalAutoCommit) {
                            conn.setAutoCommit(true);
                        }
                        conn.close();
                    }
                } catch (SQLException closeEx) {
                    LOGGER.warn("Failed to close JDBC resource while undo ... ", closeEx);
                }
            }
        }
    }

    /**
     * 当操作已完成时, 添加一条Undo log 标识(防悬挂)
     * insert uodo log when global finished
     *
     * @param xid           the xid
     * @param branchId      the branchId
     * @param undoLogParser the undoLogParse
     * @param conn          sql connection
     * @throws SQLException
     */
    protected abstract void insertUndoLogWithGlobalFinished(String xid, long branchId, UndoLogParser undoLogParser,
                                                            Connection conn) throws SQLException;

    /**
     * 当状态正常时, 插入Undo log
     * insert uodo log when normal
     *
     * @param xid            the xid
     * @param branchId       the branchId
     * @param rollbackCtx    the rollbackContext
     * @param undoLogContent the undoLogContent
     * @param conn           sql connection
     * @throws SQLException
     */
    protected abstract void insertUndoLogWithNormal(String xid, long branchId, String rollbackCtx,
                                                    byte[] undoLogContent, Connection conn) throws SQLException;

    /**
     * RollbackInfo to bytes
     *
     * @param rs
     * @return
     * @throws SQLException
     */
    protected abstract byte[] getRollbackInfo(ResultSet rs) throws SQLException;
}
