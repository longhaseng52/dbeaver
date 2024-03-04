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
package org.jkiss.dbeaver.ext.cubrid.model.meta;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.cubrid.CubridConstants;
import org.jkiss.dbeaver.ext.cubrid.CubridSQLFormatter;
import org.jkiss.dbeaver.ext.cubrid.model.CubridProcedure;
import org.jkiss.dbeaver.ext.cubrid.model.CubridSequence;
import org.jkiss.dbeaver.ext.cubrid.model.CubridSynonym;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTable;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTableColumn;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTrigger;
import org.jkiss.dbeaver.ext.cubrid.model.CubridUser;
import org.jkiss.dbeaver.ext.cubrid.model.CubridView;
import org.jkiss.dbeaver.ext.generic.model.*;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaObject;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCConstants;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSEntityConstraintType;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CubridMetaModel extends GenericMetaModel
{
    private static final Log log = Log.getLog(CubridMetaModel.class);

    public String getTableOrViewName(GenericTableBase base) 
    {
        if (base != null) {
            if (base.isView()) {
                return ((CubridView) base).getUniqueName();
            } else {
                return ((CubridTable) base).getUniqueName();
            }
        }
        return null;
    }

    @Override
    public List<GenericSchema> loadSchemas(JDBCSession session, GenericDataSource dataSource, GenericCatalog catalog)
            throws DBException
    {
        List<GenericSchema> users = new ArrayList<>();
        try {
            final JDBCPreparedStatement dbStat = session.prepareStatement("select * from db_user");
            dbStat.executeStatement();
            JDBCResultSet dbResult = dbStat.getResultSet();
            try {
                while (dbResult.next()) {
                    String name = JDBCUtils.safeGetStringTrimmed(dbResult, "name");
                    String description = JDBCUtils.safeGetStringTrimmed(dbResult, "comment");
                    CubridUser user = new CubridUser(dataSource, name, description);
                    users.add(user);
	            }
            } finally {
                dbStat.close();
            }
        } catch (SQLException e) {
            log.error("Cannot load user", e);
        }
        return users;
    }

    @Override
    public JDBCStatement prepareTableLoadStatement(
            @NotNull JDBCSession session,
            @NotNull GenericStructContainer owner,
            @Nullable GenericTableBase object,
            @Nullable String objectName)
            throws SQLException
    {
        String sql = "select a.*,a.class_name as TABLE_NAME, case when class_type = 'CLASS' then 'TABLE' \r\n"
                + "when class_type = 'VCLASS' then 'VIEW' end as TABLE_TYPE, \r\n"
                + "a.comment as REMARKS, b.current_val from db_class a LEFT JOIN \r\n"
                + "db_serial b on a.class_name = b.class_name "
                + "where a.owner_name = ?";

        final JDBCPreparedStatement dbStat = session.prepareStatement(sql);
        dbStat.setString(1, owner.getName());
        return dbStat;
    }

    @Override
    public JDBCStatement prepareTableColumnLoadStatement(
            @NotNull JDBCSession session,
            @NotNull GenericStructContainer owner,
            @Nullable GenericTableBase forTable)
            throws SQLException
    {
        String sql = "show full columns from " + getTableOrViewName(forTable);
        final JDBCPreparedStatement dbStat = session.prepareStatement(sql);
        return dbStat;
    }

    @Override
    public JDBCStatement prepareUniqueConstraintsLoadStatement(
            @NotNull JDBCSession session,
            @NotNull GenericStructContainer owner,
            @Nullable GenericTableBase forTable)
            throws SQLException, DBException
    {
        CubridTable table = (CubridTable) forTable;
        String sql = "select *, t1.index_name as PK_NAME from db_index t1 join db_index_key t2 \n"
                + "on t1.index_name = t2.index_name where is_unique = 'YES' and t1.class_name = ? \n"
                + (table.getDataSource().getSupportMultiSchema() ? "and t1.owner_name = ?" : "");
        final JDBCPreparedStatement dbStat = session.prepareStatement(sql);
        dbStat.setString(1, table.getName());
        if(table.getDataSource().getSupportMultiSchema()) {
            dbStat.setString(2, table.getSchema().getName());
        }
        return dbStat;
    }

    @Override
    public GenericTableConstraintColumn[] createConstraintColumnsImpl(
            JDBCSession session,
            GenericTableBase parent,
            GenericUniqueKey object,
            GenericMetaObject pkObject,
            JDBCResultSet dbResult)
            throws DBException
    {
        String name = JDBCUtils.safeGetStringTrimmed(dbResult, "key_attr_name");
        Integer keyOrder = JDBCUtils.safeGetInteger(dbResult, "key_order") + 1;
        GenericTableColumn tableColumn = parent.getAttribute(session.getProgressMonitor(), name);
        return new GenericTableConstraintColumn[] {
              new GenericTableConstraintColumn(object, tableColumn, keyOrder) };
    }

    @Override
    public DBSEntityConstraintType getUniqueConstraintType(JDBCResultSet dbResult) throws DBException
    {
        String isUnique = JDBCUtils.safeGetString(dbResult, "is_unique");
        String isPrimary = JDBCUtils.safeGetString(dbResult, "is_primary_key");
        if(isPrimary.equals("YES")) {
            return DBSEntityConstraintType.PRIMARY_KEY;
        } else if (isUnique.equals("YES")) {
            return DBSEntityConstraintType.UNIQUE_KEY;
        } else {
            return null;
        }
    }

    @Override
    public JDBCStatement prepareForeignKeysLoadStatement(
            @NotNull JDBCSession session,
            @NotNull GenericStructContainer owner,
            @Nullable GenericTableBase forTable)
            throws SQLException
    {
        return session.getMetaData().getImportedKeys(null, null, this.getTableOrViewName(forTable)).getSourceStatement();
    }

    @Override
    public GenericTableBase createTableImpl(
            @NotNull JDBCSession session,
            @NotNull GenericStructContainer owner,
            @NotNull GenericMetaObject tableObject,
            @NotNull JDBCResultSet dbResult)
    {
        String tableName = JDBCUtils.safeGetString(dbResult, JDBCConstants.TABLE_NAME);
        String tableType = JDBCUtils.safeGetStringTrimmed(dbResult, JDBCConstants.TABLE_TYPE);
        GenericTableBase table = createTableOrViewImpl(owner, tableName, tableType, dbResult);
        return table;
    }

    @Override
    public GenericTableBase createTableOrViewImpl(
            GenericStructContainer container,
            @Nullable String tableName,
            @Nullable String tableType,
            @Nullable JDBCResultSet dbResult)
    {
        if (tableType != null && isView(tableType)) {
            return new CubridView(container, tableName, tableType, dbResult);
        }
        return new CubridTable(container, tableName, tableType, dbResult);
    }

    public boolean isTableCommentEditable()
    {
        return true;
    }

    public boolean isTableColumnCommentEditable()
    {
        return true;
    }

    @Override
    public GenericTableColumn createTableColumnImpl(
            @NotNull DBRProgressMonitor monitor,
            @Nullable JDBCResultSet dbResult,
            @NotNull GenericTableBase table,
            String columnName,
            String typeName,
            int valueType,
            int sourceType,
            int ordinalPos,
            long columnSize,
            long charLength,
            Integer scale,
            Integer precision,
            int radix,
            boolean notNull,
            String remarks,
            String defaultValue,
            boolean autoIncrement,
            boolean autoGenerated)
            throws DBException
    {
        return new CubridTableColumn(table, dbResult);
    }

    @Override
    public String getViewDDL(DBRProgressMonitor monitor, GenericView object, Map<String, Object> options) throws DBException
    {
        JDBCSession session = DBUtils.openMetaSession(monitor, object, "load view ddl " + object.getName());
        String ddl = "-- View definition not available";
        String regex = "\\[|\\]";
        try {
            String sql= String.format("show create view %s", object.getName());
            JDBCPreparedStatement dbStat = session.prepareStatement(sql);
            dbStat.executeStatement();
            JDBCResultSet dbResult = dbStat.getResultSet();

            while(dbResult.next()) {
                ddl = "create or replace view \"" + dbResult.getString("View") + "\" as " + dbResult.getString("Create View");
                ddl = ddl.replaceAll(regex, "\"");
                ddl = new CubridSQLFormatter(ddl).format();
                break;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ddl;
    }

    @Override
    public JDBCStatement prepareSequencesLoadStatement(
            @NotNull JDBCSession session,
            @NotNull GenericStructContainer container)
            throws SQLException
    {
        String sql = "select *, owner.name from db_serial where owner.name = ?";
        final JDBCPreparedStatement dbStat = session.prepareStatement(sql);
        dbStat.setString(1, container.getName());
        return dbStat;
    }

    @Override
    public GenericSequence createSequenceImpl(
            @NotNull JDBCSession session,
            @NotNull GenericStructContainer container,
            @NotNull JDBCResultSet dbResult)
            throws DBException
    {
        String name = JDBCUtils.safeGetStringTrimmed(dbResult, "name");
        String description = JDBCUtils.safeGetString(dbResult, "comment");
        Number lastValue = JDBCUtils.safeGetInteger(dbResult, "current_val");
        Number minValue = JDBCUtils.safeGetInteger(dbResult, "min_val");
        Number maxValue = JDBCUtils.safeGetInteger(dbResult, "max_val");
        Number incrementBy = JDBCUtils.safeGetInteger(dbResult, "increment_val");
        return new CubridSequence(container, name, description, lastValue, minValue, maxValue, incrementBy, dbResult);
    }

    @Override
    public JDBCStatement prepareSynonymsLoadStatement(
            @NotNull JDBCSession session,
            @NotNull GenericStructContainer container)
            throws SQLException
    {
        String sql = "select * from db_synonym where synonym_owner_name = ?";
        final JDBCPreparedStatement dbStat = session.prepareStatement(sql);
        dbStat.setString(1, container.getName());
        return dbStat;
    }

    @Override
    public GenericSynonym createSynonymImpl(
            @NotNull JDBCSession session,
            @NotNull GenericStructContainer container,
            @NotNull JDBCResultSet dbResult)
            throws DBException
    {
        String name = JDBCUtils.safeGetStringTrimmed(dbResult, "synonym_name");
        String description = JDBCUtils.safeGetString(dbResult, "comment");
        return new CubridSynonym(container, name, description, dbResult);
    }

    @Override
    public boolean supportsDatabaseTriggers(@NotNull GenericDataSource dataSource)
    {
        return true;
    }

    @Override
    public boolean supportsTriggers(@NotNull GenericDataSource dataSource)
    {
        return true;
    }

    @Override
    public JDBCStatement prepareTableTriggersLoadStatement(
            @NotNull JDBCSession session,
            @NotNull GenericStructContainer container,
            @Nullable GenericTableBase table)
            throws SQLException
    {
        String sql = "select t1.*, t2.*, owner.name from db_trigger t1 join db_trig t2 \n"
                + "on t1.name = t2.trigger_name where owner.name = ? \n"
                + (table != null ? "and target_class_name = ?" : "");
        final JDBCPreparedStatement dbStat = session.prepareStatement(sql);
        dbStat.setString(1, container.getName());
        if (table != null) {
            dbStat.setString(2, table.getName());
        }
        return dbStat;
    }

    @Override
    public CubridTrigger createTableTriggerImpl(
            @NotNull JDBCSession session,
            @NotNull GenericStructContainer container,
            @NotNull GenericTableBase table,
            String triggerName,
            @NotNull JDBCResultSet dbResult)
            throws DBException
    {
        String name = JDBCUtils.safeGetString(dbResult, "name");
        String description = JDBCUtils.safeGetString(dbResult, "comment");
        return new CubridTrigger(table, name, description, dbResult);
    }

    @Override
    public JDBCStatement prepareContainerTriggersLoadStatement(
            @NotNull JDBCSession session,
            @Nullable GenericStructContainer container)
            throws SQLException
    {
        String sql = "select t1.*, t2.*, owner.name from db_trigger t1 join db_trig t2 \n"
                + "on t1.name = t2.trigger_name where owner.name = ? \n";
        final JDBCPreparedStatement dbStat = session.prepareStatement(sql);
        dbStat.setString(1, container.getName());
        return dbStat;
    }

    @Override
    public CubridTrigger createContainerTriggerImpl(
            @NotNull GenericStructContainer container,
            @NotNull JDBCResultSet dbResult)
            throws DBException
    {
        String name = JDBCUtils.safeGetString(dbResult, "name");
        String description = JDBCUtils.safeGetString(dbResult, "comment");
        String tableName = JDBCUtils.safeGetString(dbResult, "target_class_name");
        DBRProgressMonitor monitor = dbResult.getSession().getProgressMonitor();
        CubridTable cubridTable = null;
        for (GenericTableBase table: container.getTables(monitor)){
            if (table.getName().equals(tableName)) {
                cubridTable = (CubridTable) table;
                break;
            }
        }
        return new CubridTrigger(cubridTable, name, description, dbResult);
    }

    @Override
    public void loadProcedures(
            DBRProgressMonitor monitor,
            @NotNull GenericObjectContainer container)
            throws DBException
    {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, container, "Load procedures")) {
            String sql = "select * from db_stored_procedure where owner = ?";
            final JDBCPreparedStatement dbStat = session.prepareStatement(sql);
            dbStat.setString(1, container.getName());
            JDBCResultSet dbResult = dbStat.executeQuery();

            while (dbResult.next()) {
                String procedureName = JDBCUtils.safeGetString(dbResult, "sp_name");
                String description = JDBCUtils.safeGetString(dbResult, "comment");
                String type = JDBCUtils.safeGetString(dbResult, "sp_type");
                String target = JDBCUtils.safeGetString(dbResult, "target");
                String returnType = JDBCUtils.safeGetString(dbResult, "return_type");

                DBSProcedureType procedureType;
                if (type.equalsIgnoreCase(CubridConstants.TERM_PROCEDURE)) {
                    procedureType = DBSProcedureType.PROCEDURE;
                } else if (type.equalsIgnoreCase(CubridConstants.TERM_FUNCTION)) {
                    procedureType = DBSProcedureType.FUNCTION;
                } else {
                    procedureType = DBSProcedureType.UNKNOWN;
                }
                container.addProcedure(new CubridProcedure(container, procedureName, description, procedureType, target, returnType));
            }
        } catch (SQLException e) {
            throw new DBException("Load procedures failed", e);
        }
    }
}
