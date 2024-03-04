/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2024 DBeaver Corp and others
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

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTable;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTableColumn;
import org.jkiss.dbeaver.ext.generic.edit.GenericTableColumnManager;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.GenericTableColumn;
import org.jkiss.dbeaver.model.DBConstants;
import org.jkiss.dbeaver.model.DBPDataKind;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.edit.DBECommandContext;
import org.jkiss.dbeaver.model.edit.DBEObjectRenamer;
import org.jkiss.dbeaver.model.edit.DBEPersistAction;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.impl.edit.DBECommandAbstract;
import org.jkiss.dbeaver.model.impl.edit.SQLDatabasePersistAction;
import org.jkiss.dbeaver.model.impl.sql.edit.SQLObjectEditor;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLUtils;
import org.jkiss.dbeaver.model.struct.DBSDataType;
import org.jkiss.utils.CommonUtils;

import java.util.List;
import java.util.Map;

public class CubridTableColumnManager extends GenericTableColumnManager implements DBEObjectRenamer<GenericTableColumn>
{
    @Override
    protected CubridTableColumn createDatabaseObject(
            @NotNull DBRProgressMonitor monitor,
            @NotNull DBECommandContext context,
            Object container,
            Object copyFrom,
            @NotNull Map<String, Object> options)
            throws DBException
    {
        CubridTable table = (CubridTable) container;
        DBSDataType columnType = findBestDataType(table, DBConstants.DEFAULT_DATATYPE_NAMES);
        int columnSize = columnType != null && columnType.getDataKind() == DBPDataKind.STRING ? 100 : 0;

        CubridTableColumn column = new CubridTableColumn(table, null);
        column.setName(getNewColumnName(monitor, context, table));
        column.setTypeName(columnType == null ? "INTEGER" : columnType.getName());
        column.setMaxLength(columnSize);
        column.setRequired(false);
        column.setDescription(null);
        column.setDefaultValue(null);
        column.setAutoIncrement(false);
        column.setPersisted(false);
        return column;
    }

    @Override
    public StringBuilder getNestedDeclaration(
            DBRProgressMonitor monitor,
            GenericTableBase owner,
            DBECommandAbstract<GenericTableColumn> command,
            Map<String, Object> options)
    {
        StringBuilder decl = new StringBuilder(40);
        CubridTableColumn column = (CubridTableColumn) command.getObject();
        String columnName = DBUtils.getQuotedIdentifier(column.getDataSource(), column.getName());

        if (command instanceof SQLObjectEditor.ObjectRenameCommand) {
            columnName = DBUtils.getQuotedIdentifier(column.getDataSource(), ((ObjectRenameCommand) command).getNewName());
        }
        decl.append(columnName);
        for (ColumnModifier<GenericTableColumn> modifier : new ColumnModifier[]{DataTypeModifier, NullNotNullModifierConditional}) {
            modifier.appendModifier(monitor, column, decl, command);
        }
        if (!CommonUtils.isEmpty(column.getDefaultValue())) {
            decl.append(" DEFAULT ").append(SQLUtils.quoteString(column, column.getDefaultValue()));
        }
        if (column.isAutoIncrement() && (column.getTypeName().equals("INTEGER") || column.getTypeName().equals("BIGINT"))) {
            decl.append(" AUTO_INCREMENT");
        }
        if (!CommonUtils.isEmpty(column.getDescription())) {
            decl.append(" COMMENT ").append(SQLUtils.quoteString(column, column.getDescription()));
        }
        return decl;
    }

    @Override
    protected void addObjectModifyActions(
            DBRProgressMonitor monitor,
            DBCExecutionContext executionContext,
            List<DBEPersistAction> actionList,
            ObjectChangeCommand command,
            Map<String, Object> options)
            throws DBException
    {
        final CubridTableColumn column = (CubridTableColumn) command.getObject();
        String table = column.getTable().getSchema().getName() + "." + column.getTable().getName();
        actionList.add(
                new SQLDatabasePersistAction(
                        "Modify column",
                        "ALTER TABLE " + table + " MODIFY " + getNestedDeclaration(monitor, column.getTable(), command, options)));
    }

    @Override
    protected void addObjectRenameActions(
            DBRProgressMonitor monitor,
            DBCExecutionContext executionContext,
            List<DBEPersistAction> actions,
            ObjectRenameCommand command,
            Map<String, Object> options)
    {
        final CubridTableColumn column = (CubridTableColumn) command.getObject();
        String table = column.getTable().getSchema().getName() + "." + column.getTable().getName();
        actions.add(
                new SQLDatabasePersistAction(
                        "Rename column",
                        "ALTER TABLE " + table + " RENAME COLUMN " + command.getOldName() + " AS " + command.getNewName()));
    }

    @Override
    public void renameObject(
            DBECommandContext commandContext,
            GenericTableColumn object,
            Map<String, Object> options,
            String newName)
            throws DBException
    {
        processObjectRename(commandContext, object, options, newName);
    }
}
