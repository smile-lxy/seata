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
package io.seata.sqlparser.druid.mysql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import io.seata.sqlparser.ParametersHolder;
import io.seata.sqlparser.SQLParsingException;
import io.seata.sqlparser.SQLSelectRecognizer;
import io.seata.sqlparser.SQLType;

import java.util.ArrayList;
import java.util.List;

/**
 * MySQL Select for Update SQL语句 Where条件识别器
 * The type My sql select for update recognizer.
 *
 * @author sharajava
 */
public class MySQLSelectForUpdateRecognizer extends BaseMySQLRecognizer implements SQLSelectRecognizer {

    /**
     * SQL声明(抽象语法树)
     * https://github.com/alibaba/druid/wiki/Druid_SQL_AST#24-sqlselect--sqlselectquery
     * AbstractSyntaxTree ast
     */
    private final SQLSelectStatement ast;

    /**
     * Instantiates a new My sql select for update recognizer.
     *
     * @param originalSQL the original sql
     * @param ast         the ast
     */
    public MySQLSelectForUpdateRecognizer(String originalSQL, SQLStatement ast) {
        super(originalSQL);
        this.ast = (SQLSelectStatement)ast;
    }

    @Override
    public SQLType getSQLType() {
        return SQLType.SELECT_FOR_UPDATE;
    }

    @Override
    public String getWhereCondition(final ParametersHolder parametersHolder,
                                    final ArrayList<List<Object>> paramAppenderList) {
        SQLSelectQueryBlock selectQueryBlock = getSelect();
        SQLExpr where = selectQueryBlock.getWhere();
        return super.getWhereCondition(where, parametersHolder, paramAppenderList);
    }

    @Override
    public String getWhereCondition() {
        // 获取 select SQL语句
        SQLSelectQueryBlock selectQueryBlock = getSelect();
        // Where条件
        SQLExpr where = selectQueryBlock.getWhere();
        return super.getWhereCondition(where);
    }

    /**
     * 获取 select SQL语句
     * select name, count from undo_log where id = 1
     * select name, count from undo_log where id = 1
     * @return
     */
    private SQLSelectQueryBlock getSelect() {
        // 获取SQL查询语句
        SQLSelect select = ast.getSelect();
        if (select == null) {
            throw new SQLParsingException("should never happen!");
        }
        // 获取查询SQL
        SQLSelectQueryBlock selectQueryBlock = select.getQueryBlock();
        if (selectQueryBlock == null) {
            throw new SQLParsingException("should never happen!");
        }
        return selectQueryBlock;
    }

    @Override
    public String getTableAlias() {
        SQLSelectQueryBlock selectQueryBlock = getSelect();
        SQLTableSource tableSource = selectQueryBlock.getFrom();
        return tableSource.getAlias();
    }

    @Override
    public String getTableName() {
        SQLSelectQueryBlock selectQueryBlock = getSelect();
        SQLTableSource tableSource = selectQueryBlock.getFrom();
        StringBuilder sb = new StringBuilder();
        MySqlOutputVisitor visitor = new MySqlOutputVisitor(sb) {

            @Override
            public boolean visit(SQLExprTableSource x) {
                printTableSourceExpr(x.getExpr());
                return false;
            }
        };
        visitor.visit((SQLExprTableSource)tableSource);
        return sb.toString();
    }

}
