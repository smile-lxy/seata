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
package io.seata.sqlparser;

/**
 * SQL识别器
 * The interface Sql recognizer.
 *
 * @author sharajava
 */
public interface SQLRecognizer {

    /**
     * 获取SQL语句类型
     * Type of the SQL. INSERT/UPDATE/DELETE ...
     *
     * @return sql type
     */
    SQLType getSQLType();

    /**
     * 获取表别名
     * TableRecords source related in the SQL, including alias if any.
     * SELECT id, name FROM user u WHERE ...
     * Alias should be 'u' for this SQL.
     *
     * @return table source.
     */
    String getTableAlias();

    /**
     * 获取表名
     * TableRecords name related in the SQL.
     * SELECT id, name FROM user u WHERE ...
     * TableRecords name should be 'user' for this SQL, without alias 'u'.
     *
     * @return table name.
     * @see #getTableAlias()
     */
    String getTableName();

    /**
     * 获取原始数据
     * Return the original SQL input by the upper application.
     *
     * @return The original SQL.
     */
    String getOriginalSQL();
}
