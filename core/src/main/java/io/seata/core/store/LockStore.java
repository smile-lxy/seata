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
package io.seata.core.store;

import java.util.List;

/**
 * The interface Lock store.
 *
 * @author zhangsen
 */
public interface LockStore {

    /**
     * 加锁 (row_key)
     * Acquire lock boolean.
     *
     * @param lockDO the lock do
     * @return the boolean
     */
    boolean acquireLock(LockDO lockDO);


    /**
     * 批量加锁 (row_key)
     * Acquire lock boolean.
     *
     * @param lockDOs the lock d os
     * @return the boolean
     */
    boolean acquireLock(List<LockDO> lockDOs);

    /**
     * Un lock boolean.
     *
     * @param lockDO the lock do
     * @return the boolean
     */
    boolean unLock(LockDO lockDO);

    /**
     * 释放全局事务下多个行锁 (row_key)
     * Un lock boolean.
     *
     * @param lockDOs the lock d os
     * @return the boolean
     */
    boolean unLock(List<LockDO> lockDOs);

    /**
     * 释放全局事务下单个Branch锁
     */
    boolean unLock(String xid, Long branchId);

    /**
     * 释放全局事务下多个Branch锁
     */
    boolean unLock(String xid, List<Long> branchIds);

    /**
     * 判断row_key是否已被锁
     * Is lockable boolean.
     *
     * @param lockDOs the lock do
     * @return the boolean
     */
    boolean isLockable(List<LockDO> lockDOs);
}
