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
package io.seata.sqlparser.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;
import io.seata.sqlparser.SQLRecognizer;

/**
 * 基础识别器
 * The type Base recognizer.
 *
 * @author sharajava
 */
public abstract class BaseRecognizer implements SQLRecognizer {

    /**
     * 占位符
     * The type V marker.
     */
    public static class VMarker {
        @Override
        public String toString() {
            return "?";
        }

    }

    /**
     * 原始SQL语句
     * The Original sql.
     */
    protected String originalSQL;

    /**
     * Instantiates a new Base recognizer.
     *
     * @param originalSQL the original sql
     */
    public BaseRecognizer(String originalSQL) {
        this.originalSQL = originalSQL;

    }

    public void executeVisit(SQLExpr where, SQLASTVisitor visitor) {
        // 根据不同的Where Expr解析
        if (where instanceof SQLBinaryOpExpr) {
            // id = 1
            visitor.visit((SQLBinaryOpExpr) where);
        } else if (where instanceof SQLInListExpr) {
            // id in (1, 2, ...)
            visitor.visit((SQLInListExpr) where);
        } else if (where instanceof SQLBetweenExpr) {
            // id between 1 and 10
            visitor.visit((SQLBetweenExpr) where);
        } else if (where instanceof SQLExistsExpr) {
            // id is exist
            visitor.visit((SQLExistsExpr) where);
        } else {
            throw new IllegalArgumentException("unexpected WHERE expr: " + where.getClass().getSimpleName());
        }
    }

    @Override
    public String getOriginalSQL() {
        return originalSQL;
    }
}
