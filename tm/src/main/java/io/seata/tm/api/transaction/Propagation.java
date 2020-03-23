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
package io.seata.tm.api.transaction;

/**
 * 事务传播等级
 * @see org.springframework.transaction.annotation.Propagation
 * Propagation level of global transactions.
 *
 * @author haozhibei
 */
public enum Propagation {
    /**
     * 支持型, 不存在则创建新事务
     * The REQUIRED.
     */
    REQUIRED,

    /**
     * 新事务, 当前存在事务, 挂起
     * The REQUIRES_NEW.
     */
    REQUIRES_NEW,

    /**
     * 非事务型执行, 当前存在事务, 挂起
     * The NOT_SUPPORTED
     */
    NOT_SUPPORTED,

    /**
     * 支持型, 不存在则以非事务执行
     * The SUPPORTS
     */
    SUPPORTS,

    /**
     * 非事务方式执行, 当前存在事务, 抛出
     * The NEVER
     */
    NEVER,

    /**
     * 支持当前事务, 当前不存在事务, 抛出
     * The MANDATORY
     */
    MANDATORY

}

