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
package io.seata.rm.datasource.exec;

import io.seata.common.util.CollectionUtils;
import io.seata.common.util.IOUtil;
import io.seata.common.util.StringUtils;
import io.seata.core.context.RootContext;
import io.seata.rm.datasource.ColumnUtils;
import io.seata.rm.datasource.ConnectionProxy;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.sql.struct.Field;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableMetaCacheFactory;
import io.seata.rm.datasource.sql.struct.TableRecords;
import io.seata.rm.datasource.undo.SQLUndoLog;
import io.seata.sqlparser.ParametersHolder;
import io.seata.sqlparser.SQLRecognizer;
import io.seata.sqlparser.SQLType;
import io.seata.sqlparser.WhereRecognizer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * The type Base transactional executor.
 *
 * @param <T> the type parameter
 * @param <S> the type parameter
 * @author sharajava
 */
public abstract class BaseTransactionalExecutor<T, S extends Statement> implements Executor<T> {

    /**
     * The Statement proxy.
     */
    protected StatementProxy<S> statementProxy;

    /**
     * The Statement callback.
     */
    protected StatementCallback<T, S> statementCallback;

    /**
     * The Sql recognizer.
     */
    protected SQLRecognizer sqlRecognizer;

    /**
     * The Sql recognizer.
     */
    protected List<SQLRecognizer> sqlRecognizers;

    private TableMeta tableMeta;

    /**
     * Instantiates a new Base transactional executor.
     *
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param sqlRecognizer     the sql recognizer
     */
    public BaseTransactionalExecutor(StatementProxy<S> statementProxy, StatementCallback<T, S> statementCallback,
                                     SQLRecognizer sqlRecognizer) {
        this.statementProxy = statementProxy;
        this.statementCallback = statementCallback;
        this.sqlRecognizer = sqlRecognizer;
    }

    /**
     * Instantiates a new Base transactional executor.
     *
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param sqlRecognizer     the multi sql recognizer
     */
    public BaseTransactionalExecutor(StatementProxy<S> statementProxy, StatementCallback<T, S> statementCallback,
                                     List<SQLRecognizer> sqlRecognizers) {
        this.statementProxy = statementProxy;
        this.statementCallback = statementCallback;
        this.sqlRecognizers = sqlRecognizers;
    }

    @Override
    public T execute(Object... args) throws Throwable {
        if (RootContext.inGlobalTransaction()) {
            // 如果在全局事务里, 绑定事务
            String xid = RootContext.getXID();
            statementProxy.getConnectionProxy().bind(xid);
        }
        // 设置全局锁
        statementProxy.getConnectionProxy().setGlobalLockRequire(RootContext.requireGlobalLock());
        return doExecute(args);
    }

    /**
     * Do execute object.
     *
     * @param args the args
     * @return the object
     * @throws Throwable the throwable
     */
    protected abstract T doExecute(Object... args) throws Throwable;

    /**
     * Build where condition by p ks string.
     *
     * @param pkRows the pk rows
     * @return the string
     * @throws SQLException the sql exception
     */
    protected String buildWhereConditionByPKs(List<Field> pkRows) throws SQLException {
        // id in ( )
        StringJoiner whereConditionAppender = new StringJoiner(",", getColumnNameInSQL(pkRows.get(0).getName()) + " in (", ")");
        for (Field field : pkRows) {
            // id in (?, ?, ...)
            whereConditionAppender.add("?");
        }
        return whereConditionAppender.toString();

    }

    /**
     * build buildWhereCondition
     *
     * @param recognizer        the recognizer
     * @param paramAppenderList the param paramAppender list
     * @return the string
     */
    protected String buildWhereCondition(WhereRecognizer recognizer, ArrayList<List<Object>> paramAppenderList) {
        String whereCondition = null;
        if (statementProxy instanceof ParametersHolder) {
            whereCondition = recognizer.getWhereCondition((ParametersHolder) statementProxy, paramAppenderList);
        } else {
            whereCondition = recognizer.getWhereCondition();
        }
        //process batch operation 多个追加参数时, 用() or ( condition ) or ( condition ) ... 拼接
        if (StringUtils.isNotBlank(whereCondition) && CollectionUtils.isNotEmpty(paramAppenderList) && paramAppenderList.size() > 1) {
            StringBuilder whereConditionSb = new StringBuilder();
            whereConditionSb.append(" ( ").append(whereCondition).append(" ) ");
            for (int i = 1; i < paramAppenderList.size(); i++) {
                whereConditionSb.append(" or ( ").append(whereCondition).append(" ) ");
            }
            whereCondition = whereConditionSb.toString();
        }
        return whereCondition;
    }

    /**
     * Gets column name in sql.
     *
     * @param columnName the column name
     * @return the column name in sql
     */
    protected String getColumnNameInSQL(String columnName) {
        String tableAlias = sqlRecognizer.getTableAlias();
        return tableAlias == null ? columnName : tableAlias + "." + columnName;
    }

    /**
     * Gets from table in sql.
     *
     * @return the from table in sql
     */
    protected String getFromTableInSQL() {
        String tableName = sqlRecognizer.getTableName();
        String tableAlias = sqlRecognizer.getTableAlias();
        return tableAlias == null ? tableName : tableName + " " + tableAlias;
    }

    /**
     * Gets table meta.
     *
     * @return the table meta
     */
    protected TableMeta getTableMeta() {
        return getTableMeta(sqlRecognizer.getTableName());
    }

    /**
     * 表元信息
     * Gets table meta.
     *
     * @param tableName the table name
     * @return the table meta
     */
    protected TableMeta getTableMeta(String tableName) {
        if (tableMeta != null) {
            return tableMeta;
        }
        ConnectionProxy connectionProxy = statementProxy.getConnectionProxy();
        tableMeta = TableMetaCacheFactory.getTableMetaCache(connectionProxy.getDbType())
                .getTableMeta(connectionProxy.getTargetConnection(), tableName,
                    connectionProxy.getDataSourceProxy().getResourceId());
        return tableMeta;
    }

    /**
     * the columns contains table meta pk
     *
     * @param columns the column name list
     * @return true: contains pk false: not contains pk
     */
    protected boolean containsPK(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return false;
        }
        // 移除转义字符
        List<String> newColumns = ColumnUtils.delEscape(columns, getDbType());
        return getTableMeta().containsPK(newColumns);
    }

    /**
     * 匹配是否相等(不区分大小写)
     * compare column name and primary key name
     *
     * @param columnName the primary key column name
     * @return true: equal false: not equal
     */
    protected boolean equalsPK(String columnName) {
        String newColumnName = ColumnUtils.delEscape(columnName, getDbType());
        return StringUtils.equalsIgnoreCase(getTableMeta().getPkName(), newColumnName);
    }

    /**
     * prepare undo log.
     *
     * @param beforeImage the before image
     * @param afterImage  the after image
     * @throws SQLException the sql exception
     */
    protected void prepareUndoLog(TableRecords beforeImage, TableRecords afterImage) throws SQLException {
        if (beforeImage.getRows().isEmpty() && afterImage.getRows().isEmpty()) {
            // 如果镜像为命中数据, 直接返回
            return;
        }

        ConnectionProxy connectionProxy = statementProxy.getConnectionProxy();

        // 锁定记录
        TableRecords lockKeyRecords = sqlRecognizer.getSQLType() == SQLType.DELETE ? beforeImage : afterImage;
        // 生成锁定Key
        String lockKeys = buildLockKey(lockKeyRecords);
        // 数据源代理添加锁定Key
        connectionProxy.appendLockKey(lockKeys);

        // 生成Undo log
        SQLUndoLog sqlUndoLog = buildUndoItem(beforeImage, afterImage);
        // 追加Undo log
        connectionProxy.appendUndoLog(sqlUndoLog);
    }

    /**
     * build lockKey
     *
     * @param rowsIncludingPK the records
     * @return the string
     */
    protected String buildLockKey(TableRecords rowsIncludingPK) {
        if (rowsIncludingPK.size() == 0) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(rowsIncludingPK.getTableMeta().getTableName());
        sb.append(":");
        int filedSequence = 0;
        List<Field> pkRows = rowsIncludingPK.pkRows();
        for (Field field : pkRows) {
            sb.append(field.getValue());
            filedSequence++;
            if (filedSequence < pkRows.size()) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    /**
     * build a SQLUndoLog
     *
     * @param beforeImage the before image
     * @param afterImage  the after image
     * @return sql undo log
     */
    protected SQLUndoLog buildUndoItem(TableRecords beforeImage, TableRecords afterImage) {
        SQLType sqlType = sqlRecognizer.getSQLType();
        String tableName = sqlRecognizer.getTableName();

        SQLUndoLog sqlUndoLog = new SQLUndoLog();
        sqlUndoLog.setSqlType(sqlType);
        sqlUndoLog.setTableName(tableName);
        sqlUndoLog.setBeforeImage(beforeImage);
        sqlUndoLog.setAfterImage(afterImage);
        return sqlUndoLog;
    }


    /**
     * build a BeforeImage
     *
     * @param tableMeta         the tableMeta
     * @param selectSQL         the selectSQL
     * @param paramAppenderList the paramAppender list
     * @return a tableRecords
     * @throws SQLException the sql exception
     */
    protected TableRecords buildTableRecords(TableMeta tableMeta, String selectSQL, ArrayList<List<Object>> paramAppenderList) throws SQLException {
        ResultSet rs = null;
        // 预编译
        try (PreparedStatement ps = statementProxy.getConnection().prepareStatement(selectSQL)) {
            if (CollectionUtils.isNotEmpty(paramAppenderList)) {
                // 填充SQL值
                for (int i = 0, ts = paramAppenderList.size(); i < ts; i++) {
                    List<Object> paramAppender = paramAppenderList.get(i);
                    for (int j = 0, ds = paramAppender.size(); j < ds; j++) {
                        ps.setObject(i * ds + j + 1, paramAppender.get(j));
                    }
                }
            }
            rs = ps.executeQuery(); // 执行
            // 构建结果
            return TableRecords.buildRecords(tableMeta, rs);
        } finally {
            IOUtil.close(rs);
        }
    }

    /**
     * 获取主键对应结果
     * build TableRecords
     *
     * @param pkValues the pkValues
     * @return return TableRecords;
     * @throws SQLException
     */
    protected TableRecords buildTableRecords(List<Object> pkValues) throws SQLException {
        // 转义后主键
        String pk = getTableMeta().getEscapePkName(getDbType());
        // select * from ${tableName} where pk in (?, ?, ?, ...)
        StringJoiner pkValuesJoiner = new StringJoiner(" , ",
                "SELECT * FROM " + getFromTableInSQL() + " WHERE " + pk + " in (", ")");
        for (Object pkValue : pkValues) {
            pkValuesJoiner.add("?");
        }

        ResultSet rs = null;
        // 预编译
        try (PreparedStatement ps = statementProxy.getConnection().prepareStatement(pkValuesJoiner.toString())) {
            // 填充SQL值
            for (int i = 1, s = pkValues.size(); i <= s; i++) {
                ps.setObject(i, pkValues.get(i - 1));
            }
            // 执行
            rs = ps.executeQuery();
            // 构建结果
            return TableRecords.buildRecords(getTableMeta(), rs);
        } finally {
            IOUtil.close(rs);
        }
    }

    /**
     * get db type
     *
     * @return
     */
    protected String getDbType() {
        return statementProxy.getConnectionProxy().getDbType();
    }

}
