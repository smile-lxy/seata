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
package io.seata.rm;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;

import io.seata.core.model.BranchType;
import io.seata.core.model.ResourceManager;
import io.seata.core.protocol.transaction.UndoLogDeleteRequest;
import io.seata.rm.datasource.DataSourceManager;
import io.seata.rm.datasource.DataSourceProxy;
import io.seata.rm.datasource.undo.UndoLogManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Rm handler at.
 *
 * @author sharajava
 */
public class RMHandlerAT extends AbstractRMHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RMHandlerAT.class);

    private static final int LIMIT_ROWS = 3000;

    @Override
    public void handle(UndoLogDeleteRequest request) {
        // 数据源管理器
        DataSourceManager dataSourceManager = (DataSourceManager)getResourceManager();
        // 数据源代理
        DataSourceProxy dataSourceProxy = dataSourceManager.get(request.getResourceId());
        if (dataSourceProxy == null) {
            LOGGER.warn("Failed to get dataSourceProxy for delete undolog on {}", request.getResourceId());
            return;
        }
        // Undo log移除日期
        Date logCreatedSave = getLogCreated(request.getSaveDays());
        Connection conn = null;
        try {
            // 数据库原始连接
            conn = dataSourceProxy.getPlainConnection();
            int deleteRows = 0;
            do {
                try {
                    // 获取Undo Log管理器, 移除${logCreatedSave}前日志
                    deleteRows = UndoLogManagerFactory.getUndoLogManager(dataSourceProxy.getDbType())
                            .deleteUndoLogByLogCreated(logCreatedSave, LIMIT_ROWS, conn);
                    if (deleteRows > 0 && !conn.getAutoCommit()) {
                        // 可删除记录 > 0 && 不是自动提交事务, 提交事务
                        conn.commit();
                    }
                } catch (SQLException exx) {
                    if (deleteRows > 0 && !conn.getAutoCommit()) {
                        // // 可删除记录 > 0 && 不是自动提交事务, 回滚事务
                        conn.rollback();
                    }
                    throw exx;
                }
            } while (deleteRows == LIMIT_ROWS);
        } catch (Exception e) {
            LOGGER.error("Failed to delete expired undo_log, error:{}", e.getMessage(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException closeEx) {
                    LOGGER.warn("Failed to close JDBC resource while deleting undo_log ", closeEx);
                }
            }
        }
    }

    /**
     * 获取Undo log移除日期
     */
    private Date getLogCreated(int saveDays) {
        if (saveDays <= 0) {
            saveDays = UndoLogDeleteRequest.DEFAULT_SAVE_DAYS;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -saveDays);
        return calendar.getTime();
    }

    /**
     * get AT resource managerDataSourceManager.java
     *
     * @return
     */
    @Override
    protected ResourceManager getResourceManager() {
        return DefaultResourceManager.get().getResourceManager(BranchType.AT);
    }

    @Override
    public BranchType getBranchType() {
        return BranchType.AT;
    }

}
