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

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTable;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTableColumn;
import org.jkiss.dbeaver.ext.generic.edit.GenericTableManager;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.GenericTableForeignKey;
import org.jkiss.dbeaver.ext.generic.model.GenericTableIndex;
import org.jkiss.dbeaver.ext.generic.model.GenericUniqueKey;
import org.jkiss.dbeaver.model.edit.DBECommandContext;
import org.jkiss.dbeaver.model.edit.DBEObjectRenamer;
import org.jkiss.dbeaver.model.edit.DBEPersistAction;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.impl.edit.SQLDatabasePersistAction;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLUtils;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.utils.CommonUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class CubridTableManager extends GenericTableManager implements DBEObjectRenamer<GenericTableBase>
{
    private static final Class<? extends DBSObject>[] CHILD_TYPES = CommonUtils.array(
            CubridTableColumn.class,
            GenericUniqueKey.class,
            GenericTableForeignKey.class,
            GenericTableIndex.class);

    @Override
    public Class<? extends DBSObject>[] getChildTypes() {
        return CHILD_TYPES;
    }

    @Override
    public Collection<? extends DBSObject> getChildObjects(DBRProgressMonitor monitor, GenericTableBase object, Class<? extends DBSObject> childType) throws DBException {
        if (childType == CubridTableColumn.class) {
            return object.getAttributes(monitor);
        }
        return super.getChildObjects(monitor, object, childType);
    }

    @Override
    protected void addObjectModifyActions(
            DBRProgressMonitor monitor,
            DBCExecutionContext executionContext,
            List<DBEPersistAction> actionList,
            ObjectChangeCommand command,
            Map<String, Object> options) {
        if (command.getProperties().size() > 1 || command.getProperty("schema") == null) {
            CubridTable table = (CubridTable) command.getObject();
            StringBuilder query = new StringBuilder("ALTER TABLE ");
            query.append(table.getOldSchema() + "." + table.getName());
            appendTableModifiers(monitor, table, command, query, true);
            actionList.add(new SQLDatabasePersistAction(query.toString()));
        }
    }

    @Override
    protected void appendTableModifiers(
            DBRProgressMonitor monitor,
            GenericTableBase genericTable,
            NestedObjectCommand command,
            StringBuilder query,
            boolean alter) {
        CubridTable table = (CubridTable) genericTable;
        String suffix = alter ? "," : "\n";
        query.append("\n");
        if (table.isReuseOID() == false && alter == false) {
            query.append("DONT_REUSE_OID").append(suffix);
        }
        if ((command.getProperty("charset") != null || command.getProperty("collation") != null) && table.getCollation().getName() != null) {
            query.append("COLLATE ").append(table.getCollation().getName()).append(suffix);
        }
        if (command.getProperty("autoIncrement") != null && table.getAutoIncrement() != 0) {
            query.append("AUTO_INCREMENT = ").append(table.getAutoIncrement()).append(suffix);
        }
        if (command.getProperty("description") != null && table.getDescription() != null) {
            query.append("COMMENT = ").append(SQLUtils.quoteString(table, CommonUtils.notEmpty(table.getDescription()))).append(suffix);
        }
        query.deleteCharAt(query.length() - 1);
    }

    @Override
    protected void addObjectExtraActions(
            DBRProgressMonitor monitor,
            DBCExecutionContext executionContext,
            List<DBEPersistAction> actions,
            NestedObjectCommand<GenericTableBase, PropertyHandler> command,
            Map<String, Object> options) {
        CubridTable table = (CubridTable) command.getObject();
        if (command.hasProperty("schema") && table.getOldSchema() != table.getSchema()) {
            actions.add(
                    new SQLDatabasePersistAction(
                            "Change Owner",
                            "ALTER TABLE " + table.getOldSchema() + "." + table.getName() + " OWNER TO " + table.getSchema()));
        }
    }

    @Override
    protected void addObjectRenameActions(
            DBRProgressMonitor monitor,
            DBCExecutionContext executionContext,
            List<DBEPersistAction> actions,
            ObjectRenameCommand command,
            Map<String, Object> options) {
        CubridTable table = (CubridTable) command.getObject();
        actions.add(
                new SQLDatabasePersistAction(
                        "Rename table",
                        "RENAME TABLE " + table.getOldSchema() + "." + command.getOldName() + " TO " + command.getNewName()));
    }

    @Override
    public void renameObject(
            DBECommandContext commandContext,
            GenericTableBase object,
            Map<String, Object> options,
            String newName)
            throws DBException {
        processObjectRename(commandContext, object, options, newName);
    }
}
