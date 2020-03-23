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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import io.seata.common.util.IOUtil;
import io.seata.rm.datasource.StatementProxy;

import io.seata.sqlparser.SQLRecognizer;
import io.seata.sqlparser.SQLUpdateRecognizer;
import io.seata.rm.datasource.sql.struct.Field;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableRecords;
import org.apache.commons.lang.StringUtils;

/**
 * The type Update executor.
 *
 * @author sharajava
 *
 * @param <T> the type parameter
 * @param <S> the type parameter
 */
public class UpdateExecutor<T, S extends Statement> extends AbstractDMLBaseExecutor<T, S> {

    /**
     * Instantiates a new Update executor.
     *
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param sqlRecognizer     the sql recognizer
     */
    public UpdateExecutor(StatementProxy<S> statementProxy, StatementCallback<T,S> statementCallback,
                          SQLRecognizer sqlRecognizer) {
        super(statementProxy, statementCallback, sqlRecognizer);
    }

    @Override
    protected TableRecords beforeImage() throws SQLException {

        ArrayList<List<Object>> paramAppenderList = new ArrayList<>(); // 参数追加集合
        TableMeta tmeta = getTableMeta(); // 表原始信息
        // 构建SQL语句
        String selectSQL = buildBeforeImageSQL(tmeta, paramAppenderList);
        // 构建结果
        return buildTableRecords(tmeta, selectSQL, paramAppenderList);
    }

    private String buildBeforeImageSQL(TableMeta tableMeta, ArrayList<List<Object>> paramAppenderList) {
        SQLUpdateRecognizer recognizer = (SQLUpdateRecognizer)sqlRecognizer;
        // 需要更新的列
        List<String> updateColumns = recognizer.getUpdateColumns();
        StringBuilder prefix = new StringBuilder("SELECT ");
        if (!containsPK(updateColumns)) {
            // 追加主键 select id,
            prefix.append(getColumnNameInSQL(tableMeta.getEscapePkName(getDbType()))).append(", ");
        }
        // from t_business
        StringBuilder suffix = new StringBuilder(" FROM ").append(getFromTableInSQL());
        // 条件
        String whereCondition = buildWhereCondition(recognizer, paramAppenderList);
        if (StringUtils.isNotBlank(whereCondition)) {
            // from t_business where ...
            suffix.append(" WHERE ").append(whereCondition);
        }
        // from t_business where ... for update
        suffix.append(" FOR UPDATE");
        // select id, from t_business where ... for update
        StringJoiner selectSQLJoin = new StringJoiner(", ", prefix.toString(), suffix.toString());
        // select id, biz_Column1, biz_Column2, ... from t_business where ... for update
        for (String updateColumn : updateColumns) {
            selectSQLJoin.add(updateColumn);
        }
        return selectSQLJoin.toString();
    }

    @Override
    protected TableRecords afterImage(TableRecords beforeImage) throws SQLException {
        // 表元信息
        TableMeta tmeta = getTableMeta();
        if (beforeImage == null || beforeImage.size() == 0) {
            return TableRecords.empty(getTableMeta());
        }
        // 构建SQL语句
        String selectSQL = buildAfterImageSQL(tmeta, beforeImage);
        ResultSet rs = null;
        // 预编译
        try (PreparedStatement pst = statementProxy.getConnection().prepareStatement(selectSQL)) {
            // 填充SQL值
            List<Field> pkRows = beforeImage.pkRows();
            for (int i = 1; i <= pkRows.size(); i++) {
                Field pkField = pkRows.get(i - 1);
                pst.setObject(i, pkField.getValue(), pkField.getType());
            }
            rs = pst.executeQuery(); // 执行
            // 构建结果
            return TableRecords.buildRecords(tmeta, rs);
        } finally {
            IOUtil.close(rs);
        }
    }

    private String buildAfterImageSQL(TableMeta tableMeta, TableRecords beforeImage) throws SQLException {
        SQLUpdateRecognizer recognizer = (SQLUpdateRecognizer)sqlRecognizer;
        // 更新列
        List<String> updateColumns = recognizer.getUpdateColumns();
        StringBuilder prefix = new StringBuilder("SELECT ");
        if (!containsPK(updateColumns)) {
            // select `id`,
            // PK should be included.
            prefix.append(getColumnNameInSQL(tableMeta.getEscapePkName(getDbType()))).append(", ");
        }
        // from t_business where
        String suffix = " FROM " + getFromTableInSQL() + " WHERE " + buildWhereConditionByPKs(beforeImage.pkRows());
        StringJoiner selectSQLJoiner = new StringJoiner(", ", prefix.toString(), suffix);
        // select `id`, ... from t_business where id in (?, ?, ...)
        for (String column : updateColumns) {
            selectSQLJoiner.add(column);
        }
        return selectSQLJoiner.toString();
    }

}
