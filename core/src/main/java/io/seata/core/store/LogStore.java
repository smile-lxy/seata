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
 * the transaction log store
 *
 * @author zhangsen
 */
public interface LogStore {

    /**
     * 查询全局事务信息
     * Query global transaction do global transaction do.
     *
     * @param xid the xid
     * @return the global transaction do
     */
    GlobalTransactionDO queryGlobalTransactionDO(String xid);

    /**
     * 查询全局事务信息
     * Query global transaction do global transaction do.
     *
     * @param transactionId the transaction id
     * @return the global transaction do
     */
    GlobalTransactionDO queryGlobalTransactionDO(long transactionId);

    /**
     * 查询全局事务列表
     * Query global transaction do list.
     *
     * @param status the status
     * @param limit  the limit
     * @return the list
     */
    List<GlobalTransactionDO> queryGlobalTransactionDO(int[] status, int limit);

    /**
     * 新增全局事务记录
     * Insert global transaction do boolean.
     *
     * @param globalTransactionDO the global transaction do
     * @return the boolean
     */
    boolean insertGlobalTransactionDO(GlobalTransactionDO globalTransactionDO);

    /**
     * 修改全局事务状态
     * Update global transaction do boolean.
     *
     * @param globalTransactionDO the global transaction do
     * @return the boolean
     */
    boolean updateGlobalTransactionDO(GlobalTransactionDO globalTransactionDO);

    /**
     * 删除全局事务记录
     * Delete global transaction do boolean.
     *
     * @param globalTransactionDO the global transaction do
     * @return the boolean
     */
    boolean deleteGlobalTransactionDO(GlobalTransactionDO globalTransactionDO);

    /**
     * 查询Branch事务列表
     * Query branch transaction do list.
     *
     * @param xid the xid
     * @return the BranchTransactionDO list
     */
    List<BranchTransactionDO> queryBranchTransactionDO(String xid);

    /**
     * 查询Branch事务列表
     * Query branch transaction do list.
     *
     * @param xids the xid list
     * @return the BranchTransactionDO list
     */
    List<BranchTransactionDO> queryBranchTransactionDO(List<String> xids);

    /**
     * 新增Branch事务记录
     * Insert branch transaction do boolean.
     *
     * @param branchTransactionDO the branch transaction do
     * @return the boolean
     */
    boolean insertBranchTransactionDO(BranchTransactionDO branchTransactionDO);

    /**
     * 修改Branch事务状态
     * Update branch transaction do boolean.
     *
     * @param branchTransactionDO the branch transaction do
     * @return the boolean
     */
    boolean updateBranchTransactionDO(BranchTransactionDO branchTransactionDO);

    /**
     * 删除Branch事务记录
     * Delete branch transaction do boolean.
     *
     * @param branchTransactionDO the branch transaction do
     * @return the boolean
     */
    boolean deleteBranchTransactionDO(BranchTransactionDO branchTransactionDO);

    /**
     * Gets current max session id.
     *
     * @param high the high
     * @param low  the low
     * @return the current max session id
     */
    long getCurrentMaxSessionId(long high, long low);

}
