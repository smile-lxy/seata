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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import io.seata.common.util.StringUtils;
import io.seata.core.context.RootContext;
import io.seata.rm.datasource.StatementProxy;
import io.seata.sqlparser.SQLRecognizer;
import io.seata.sqlparser.SQLSelectRecognizer;
import io.seata.rm.datasource.sql.struct.TableRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Select for update executor.
 *
 * @param <S> the type parameter
 * @author sharajava
 */
public class SelectForUpdateExecutor<T, S extends Statement> extends BaseTransactionalExecutor<T, S> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectForUpdateExecutor.class);

    /**
     * Instantiates a new Select for update executor.
     *
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param sqlRecognizer     the sql recognizer
     */
    public SelectForUpdateExecutor(StatementProxy<S> statementProxy, StatementCallback<T, S> statementCallback,
                                   SQLRecognizer sqlRecognizer) {
        super(statementProxy, statementCallback, sqlRecognizer);
    }

    @Override
    public T doExecute(Object... args) throws Throwable {
        // 数据源原始连接
        Connection conn = statementProxy.getConnection();
        // 数据库元信息
        DatabaseMetaData dbmd = conn.getMetaData();
        T rs;
        Savepoint sp = null;
        LockRetryController lockRetryController = new LockRetryController();
        boolean originalAutoCommit = conn.getAutoCommit();
        ArrayList<List<Object>> paramAppenderList = new ArrayList<>();
        // select for update SQL 语句
        String selectPKSQL = buildSelectSQL(paramAppenderList);
        try {
            if (originalAutoCommit) {
                // 手动提交
                /*
                 * In order to hold the local db lock during global lock checking
                 * set auto commit value to false first if original auto commit was true
                 */
                conn.setAutoCommit(false);
            } else if (dbmd.supportsSavepoints()) {
                /*
                 * 为了在全局锁冲突时释放本地数据库锁，如果原始自动提交为false，则创建一个保存点，
                 * 然后在必要时在全局锁检查期间使用此处的保存点来释放数据库锁
                 * In order to release the local db lock when global lock conflict
                 * create a save point if original auto commit was false, then use the save point here to release db
                 * lock during global lock checking if necessary
                 */
                // 保存点
                sp = conn.setSavepoint();
            } else {
                throw new SQLException("not support savepoint. please check your db version");
            }

            while (true) {
                try {
                    // 执行
                    // #870
                    // execute return Boolean
                    // executeQuery return ResultSet
                    rs = statementCallback.execute(statementProxy.getTargetStatement(), args);

                    // 尝试获取选定行的全局锁定
                    // Try to get global lock of those rows selected
                    TableRecords selectPKRows = buildTableRecords(getTableMeta(), selectPKSQL, paramAppenderList);
                    // 构建Lock key
                    String lockKeys = buildLockKey(selectPKRows);
                    if (StringUtils.isNullOrEmpty(lockKeys)) {
                        break;
                    }

                    if (RootContext.inGlobalTransaction()) {
                        // 在全局事务内, 检查全局锁
                        //do as usual
                        statementProxy.getConnectionProxy().checkLock(lockKeys);
                    } else if (RootContext.requireGlobalLock()) {
                        // 在全局锁内, 追加锁
                        //check lock key before commit just like DML to avoid reentrant lock problem(no xid thus can
                        // not reentrant)
                        statementProxy.getConnectionProxy().appendLockKey(lockKeys);
                    } else {
                        throw new RuntimeException("Unknown situation!");
                    }
                    break;
                } catch (LockConflictException lce) {
                    // 回滚事务
                    if (sp != null) {
                        conn.rollback(sp);
                    } else {
                        conn.rollback();
                    }
                    // 休息片刻, 重试
                    lockRetryController.sleep(lce);
                }
            }
        } finally {
            // 清理现场
            if (sp != null) {
                // 释放保存点
                try {
                    conn.releaseSavepoint(sp);
                } catch (SQLException e) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("{} does not support release save point, but this is not a error.", getDbType());
                    }
                }
            }
            // 恢复自动提交
            if (originalAutoCommit) {
                conn.setAutoCommit(true);
            }
        }
        return rs;
    }

    /**
     * 构建select SQL语句
     * @param paramAppenderList
     * @return
     */
    private String buildSelectSQL(ArrayList<List<Object>> paramAppenderList) {
        SQLSelectRecognizer recognizer = (SQLSelectRecognizer)sqlRecognizer;
        StringBuilder selectSQLAppender = new StringBuilder("SELECT ");
        // select ${pkName}
        selectSQLAppender.append(getColumnNameInSQL(getTableMeta().getPkName()));
        // select ${pkName} from ${tableName}
        selectSQLAppender.append(" FROM ").append(getFromTableInSQL());
        // select ${pkName} from ${tableName} where ${whereCondition}
        String whereCondition = buildWhereCondition(recognizer, paramAppenderList);
        if (StringUtils.isNotBlank(whereCondition)) {
            selectSQLAppender.append(" WHERE ").append(whereCondition);
        }
        // select ${pkName} from ${tableName} where ${whereCondition} for update
        selectSQLAppender.append(" FOR UPDATE");
        return selectSQLAppender.toString();
    }
}
