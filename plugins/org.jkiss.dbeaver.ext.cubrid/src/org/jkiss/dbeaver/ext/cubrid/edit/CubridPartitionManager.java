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
package org.jkiss.dbeaver.ext.cubrid.edit;

import java.util.List;
import java.util.Map;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.cubrid.model.CubridPartition;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTable;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTableColumn;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.model.DBPDataKind;
import org.jkiss.dbeaver.model.edit.DBECommandContext;
import org.jkiss.dbeaver.model.edit.DBEPersistAction;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.impl.edit.SQLDatabasePersistAction;
import org.jkiss.dbeaver.model.impl.sql.edit.SQLStructEditor;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLUtils;
import org.jkiss.utils.CommonUtils;

public class CubridPartitionManager extends CubridTableManager {

    @Override
    protected CubridPartition createDatabaseObject(
            @NotNull DBRProgressMonitor monitor,
            @NotNull DBECommandContext context,
            Object container,
            Object copyFrom,
            @NotNull Map<String, Object> options) {
        return new CubridPartition((CubridTable) container, "partion_name", "", null);
    }

    @Override
    public boolean canCreateObject(@NotNull Object container) {
        return true;
    }

    @Override
    public SQLStructEditor<GenericTableBase, GenericStructContainer>.StructCreateCommand makeCreateCommand(
            GenericTableBase object,
            Map<String, Object> options) {
        StructCreateCommand createCommand = super.makeCreateCommand(object, options);
        createCommand.setIgnoreNestedCommands(true);
        return createCommand;
    }

    @Override
    protected void addStructObjectCreateActions(
            DBRProgressMonitor monitor,
            DBCExecutionContext executionContext,
            List<DBEPersistAction> actions,
            StructCreateCommand command,
            Map<String, Object> options)
            throws DBException {
        StringBuilder query = new StringBuilder();
        CubridPartition currentPartition = (CubridPartition) command.getObject();
        List<CubridPartition> partitions = currentPartition.getPartitionsFromTableCache(currentPartition);
        CubridPartition firstPartition = (!CommonUtils.isEmpty(partitions)) ? partitions.get(0) : null;

        CubridTable parentTable = currentPartition.getParentTable();
        CubridTableColumn column = (CubridTableColumn) parentTable.getAttribute(monitor, currentPartition.getPartitionKey());
        String type = currentPartition.getTableType();

        query.append("ALTER TABLE ").append(parentTable.getUniqueName());
        boolean isPartitioned = parentTable.isPartitioned();
        if (isPartitioned || ("HASH".equals(type)) && firstPartition != currentPartition) {
            query.append(" ADD PARTITION (");
        } else {
            query.append(" PARTITION BY ").append(type).append(" (").append(currentPartition.getPartitionKey()).append(") (");
        }

        if ("RANGE".equals(type)) {
            if (firstPartition == currentPartition) {
                sortedPartition(partitions);
                renderRangePartitions(partitions, column, query);
                actions.add( 0, new SQLDatabasePersistAction("Create Partition", query.toString()));
            }
        } else if ("LIST".equals(type)) {
            if (firstPartition == currentPartition) {
                renderListPartitions(partitions, column, query);
                actions.add( 0, new SQLDatabasePersistAction("Create Partition", query.toString()) );
            }
        } else if ("HASH".equals(type)) {
            query.deleteCharAt(query.length() - 1);
            query.append("PARTITIONS ").append(currentPartition.getPartitionValues());
            actions.add( 0, new SQLDatabasePersistAction("Create Partition", query.toString()));
        }
    }

    private void renderRangePartitions(List<CubridPartition> partitions, CubridTableColumn column, StringBuilder query) {
        for (CubridPartition partition : partitions) {
            String partitionValues = CommonUtils.notEmpty(partition.getPartitionValues());
            query.append("\n\tPARTITION ").append(partition.getName()).append(" VALUES LESS THAN ");

            if ("MAXVALUE".equalsIgnoreCase(partitionValues)) {
                query.append("MAXVALUE");
            } else {
                query.append("(").append(DBPDataKind.NUMERIC == column.getDataKind() ?
                    partitionValues : SQLUtils.quoteString(partition, partitionValues))
                .append(")");
            }
            query.append(CommonUtils.isEmpty(partition.getDescription()) ? "" :
                " COMMENT " + SQLUtils.quoteString(partition, CommonUtils.notEmpty(partition.getDescription())));
            query.append(",");
        }
        query.deleteCharAt(query.length() - 1).append("\n)");
    }

    private void renderListPartitions(List<CubridPartition> partitions, CubridTableColumn column, StringBuilder query) {
        for (CubridPartition partition : partitions) {
            String[] partitionValues = partition.getPartitionValues().split("\\s*,\\s*");
            query.append("\n\tPARTITION ").append(partition.getName()).append(" VALUES IN ");

            if (DBPDataKind.NUMERIC == column.getDataKind()) {
                query.append("(").append(String.join(", ", partitionValues)).append(")");
            } else {
                query.append("('").append(String.join("', '", partitionValues)).append("')");
            }
            query.append(CommonUtils.isEmpty(partition.getDescription()) ? "" :
                " COMMENT " + SQLUtils.quoteString(partition, CommonUtils.notEmpty(partition.getDescription())));
            query.append(",");
        }
        query.deleteCharAt(query.length() - 1).append("\n)");
    }

    private void sortedPartition(List<CubridPartition> partitions) {
        partitions.sort((p1, p2) -> {
            String val1 = p1.getPartitionValues();
            String val2 = p2.getPartitionValues();

            boolean isNum1 = isNumeric(val1);
            boolean isNum2 = isNumeric(val2);

            if (isNum1 && isNum2) {
                return Integer.compare(Integer.parseInt(val1), Integer.parseInt(val2));
            }
            if (isNum1) return -1;
            if (isNum2) return 1;
            return val1.compareTo(val2);
        });
    }

    private static boolean isNumeric(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    protected void addObjectDeleteActions(
            @NotNull DBRProgressMonitor monitor,
            @NotNull DBCExecutionContext executionContext,
            @NotNull List<DBEPersistAction> actions,
            @NotNull ObjectDeleteCommand command,
            @NotNull Map<String, Object> options) {
        CubridPartition partition = (CubridPartition) command.getObject();
        if ("HASH".equals(partition.getTableType())) {
            actions.add(new SQLDatabasePersistAction("Drop Partition",
                    "ALTER TABLE " + partition.getParentTable().getUniqueName()
                    + " COALESCE PARTITION 1"));
        } else {
	        actions.add(new SQLDatabasePersistAction("Drop Partition",
	                "ALTER TABLE " + partition.getParentTable().getUniqueName()
	                + " DROP PARTITION " + partition.getName()));
        }
    }

    @Override
    public void renameObject(
            @NotNull DBECommandContext commandContext,
            @NotNull GenericTableBase object,
            @NotNull Map<String, Object> options,
            @NotNull String newName)
            throws DBException {
    }
}
