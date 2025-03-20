/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2025 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.ext.cubrid.ui.config;

import java.util.Map;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.ext.cubrid.model.CubridPartition;
import org.jkiss.dbeaver.ext.cubrid.ui.internal.CubridMessages;
import org.jkiss.dbeaver.model.edit.DBECommandContext;
import org.jkiss.dbeaver.model.edit.DBEObjectConfigurator;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.runtime.DBWorkbench;
import org.jkiss.dbeaver.ui.UITask;

public class CubridPartitionConfigurator implements DBEObjectConfigurator<CubridPartition> {

    @Override
    public CubridPartition configureObject(
            @NotNull DBRProgressMonitor monitor,
            @Nullable DBECommandContext commandContext,
            @Nullable Object parent,
            @NotNull CubridPartition newPartition,
            @NotNull Map<String, Object> options) {
        return UITask.run(() -> {
            if (!DBWorkbench.getPlatformUI().confirmAction(
                    CubridMessages.create_cubrid_partition_warning_title,
                    CubridMessages.create_cubrid_partition_warning_message)) {
                return null;
            }
            CreateCubridPartitionPage editPage = new CreateCubridPartitionPage(monitor, newPartition);
            if (!editPage.edit()) {
                return null;
            }
            newPartition.setName(editPage.getPartitionName());
            newPartition.setTableType(editPage.getPartitionType());
            newPartition.setPartitionKey(editPage.getPartitionKey());
            newPartition.setPartitionValues(editPage.getPartitionValues());
            newPartition.setDescription(editPage.getDescription());
            return newPartition;
        });
    }
}
