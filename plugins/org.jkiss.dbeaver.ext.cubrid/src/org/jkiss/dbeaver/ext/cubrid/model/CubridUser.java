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
package org.jkiss.dbeaver.ext.cubrid.model;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.cubrid.CubridConstants;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTable.OwnerListProvider;
import org.jkiss.dbeaver.ext.cubrid.model.meta.CubridMetaModel;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.GenericObjectContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericSynonym;
import org.jkiss.dbeaver.ext.generic.model.GenericTable;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.GenericTableColumn;
import org.jkiss.dbeaver.ext.generic.model.GenericTableIndex;
import org.jkiss.dbeaver.ext.generic.model.GenericTableIndexColumn;
import org.jkiss.dbeaver.ext.generic.model.GenericView;
import org.jkiss.dbeaver.ext.generic.model.TableCache;
import org.jkiss.dbeaver.model.DBPNamedObject2;
import org.jkiss.dbeaver.model.DBPSaveableObject;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCConstants;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCCompositeCache;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCObjectCache;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCObjectWithParentCache;
import org.jkiss.dbeaver.model.meta.IPropertyValueListProvider;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.meta.PropertyLength;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.rdb.DBSIndexType;
import org.jkiss.utils.CommonUtils;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

public class CubridUser extends GenericSchema implements DBPNamedObject2, DBPSaveableObject
{
    private String name;
    private String description;
    private String password;
    private boolean addGroup;
    private boolean persisted;
    private String group;
    private List<GenericSchema> schemas;
//    private CubridUser allGroups;
    private List<String> allGroups = new ArrayList<>();
    private List<String> groups = new ArrayList<>();
    private final CubridIndexCache cubridIndexCache;
    private final CubridTriggerCache cubridTriggerCache;
    private List<GenericSchema> groupRefs;


    public CubridUser(
            @NotNull GenericDataSource dataSource,
            @NotNull String schemaName,
            @Nullable String description,
            @Nullable JDBCResultSet dbResult,
            DBRProgressMonitor monitor) {
        super(dataSource, null, schemaName);
        this.name = schemaName;
        this.description = description;
//        this.allGroups = this;
        this.persisted = dbResult != null;
        this.cubridIndexCache = new CubridIndexCache(this.getTableCache());
//        this.cubridTriggerCache = new CubridTriggerCache(this.getTableCache());
        this.cubridTriggerCache = new CubridTriggerCache();

        if(dbResult != null) {
	        String sql = "select t.groups.name from db_user join table(groups) as t(groups) where name = ?";
	        try (JDBCPreparedStatement dbStat = dbResult.getSession().prepareStatement(sql)) {
	            dbStat.setString(1, name);
	            try (JDBCResultSet result = dbStat.executeQuery()) {
	                while (result.next()) {
	                    groups.add(JDBCUtils.safeGetString(result, "groups.name"));
	                }
	                group = String.join(", ", groups);
	            }
	        } catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        
        try {
			this.groupRefs = ((CubridDataSource) dataSource).getCubridUsers(monitor);
		} catch (DBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        for (GenericSchema col : CommonUtils.safeCollection(groupRefs)) {
            this.allGroups.add(col.getName());
        }
//        try (JDBCPreparedStatement dbStat = dbResult.getSession().prepareStatement("SELECT name from db_user;")) {
//            try (JDBCResultSet result = dbStat.executeQuery()) {
//                while (result.next()) {
//                    allGroups.add(JDBCUtils.safeGetString(result, "name"));
//                }
//            }
//        }
////        this.schemas = dataSource.getSchemas();
////        System.out.println(schemas);
// catch (SQLException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//}
    }

    @NotNull
    @Override
    public boolean isPersisted() {
        return persisted;
    }

    @Override
    public void setPersisted(@NotNull boolean persisted) {
        this.persisted = persisted;
    }

    @NotNull
    @Property(viewable = true, editable = true, order = 1)
    public String getName() {
        return name;
    }

    public void setName(@NotNull String name) {
    	this.name = name;
    }

    @NotNull
    public boolean isPasswordEditable() {
        String currentUser = getDataSource().getContainer().getConnectionConfiguration().getUserName().toUpperCase();
        return currentUser.equals("DBA") || currentUser.equals(getName().toUpperCase());
    }

    @Nullable
    @Property(viewable = true, order = 2, editable = true, updatableExpr = "object.passwordEditable")
    public String getPassword() {
        return password;
    }

    public void setPassword(@Nullable String newPassword) {
        this.password = newPassword;
    }

    @Nullable
    @Property(viewable = true, length = PropertyLength.MULTILINE, order = 10, editable = true, updatable = true)
    public String getDescription() {
        return description;
    }

    public void setDescription(@Nullable String description) {
        this.description = description;
    }

    @Nullable
    @Property(viewable = true, order = 4, editable = true)
    public String getGroups() {
        return group;
    }

    public void setGroups(String group) {
    	this.group = group;
    }
    
//    @Property(viewable = true, order = 5, editable = true, updatable = true)
//    public boolean getAddGroup() {
//    	return addGroup;
//    }
//
//    public void setAddGroup(boolean addGroup) {
//    	this.addGroup = addGroup;
//    }
//
//    @Property(viewable = true, order = 6, editable = true, updatable = true, listProvider = GroupListProvider.class)
//    public String getAllGroup() {
////    	System.out.println(getDataSource().getSchemas());
////    	for (GenericSchema schema : getDataSource().getSchemas()) {
////    		if (!schema.getName().equals("DBA")) {
////    			allGroups.add(schema.getName());
////    		}
////    	}
//    	return addGroup ? group : null;
//    }

    @NotNull
    public boolean supportsSystemTable() {
        return name.equals("DBA");
    }

    @NotNull
    public boolean supportsSystemView() {
        return name.equals("DBA");
    }

    @NotNull
    public boolean showSystemTableFolder() {
        return this.getDataSource().getContainer().getNavigatorSettings().isShowSystemObjects();
    }

    @NotNull
    public boolean supportsSynonym() {
        return ((CubridDataSource) this.getDataSource()).getSupportMultiSchema();
    }

    @NotNull
    public boolean supportsTrigger() {
        return CubridConstants.DBA.equals(getDataSource().getContainer().getConnectionConfiguration().getUserName());
    }

    @NotNull
    @Override
    public TableCache createTableCache(@NotNull GenericDataSource datasource) {
        return new CubridTableCache(datasource);
    }

    @NotNull
    public CubridIndexCache getCubridIndexCache() {
        return cubridIndexCache;
    }

    public CubridTriggerCache getCubridTriggerCache() {
    	return cubridTriggerCache;
    }
    @Nullable
    @Override
    public List<CubridTable> getPhysicalTables(@NotNull DBRProgressMonitor monitor) throws DBException {
        List<CubridTable> tables = new ArrayList<>();
        for (GenericTable table : super.getPhysicalTables(monitor)) {
            if (!table.isSystem()) {
                tables.add((CubridTable) table);
            }
        }
        return tables;
    }

    @Nullable
    public List<? extends CubridTable> getPhysicalSystemTables(@NotNull DBRProgressMonitor monitor)
            throws DBException {
        List<CubridTable> tables = new ArrayList<>();
        for (GenericTable table : super.getPhysicalTables(monitor)) {
            if (table.isSystem()) {
                tables.add((CubridTable) table);
            }
        }
        return tables;
    }

    @Nullable
    @Override
    public List<CubridView> getViews(@NotNull DBRProgressMonitor monitor) throws DBException {
        List<CubridView> views = new ArrayList<>();
        for (GenericView view : super.getViews(monitor)) {
            if (!view.isSystem()) {
                views.add((CubridView) view);
            }
        }
        return views;
    }

    @Nullable
    public List<CubridView> getSystemViews(@NotNull DBRProgressMonitor monitor) throws DBException {
        List<CubridView> views = new ArrayList<>();
        for (GenericView view : super.getViews(monitor)) {
            if (view.isSystem()) {
                views.add((CubridView) view);
            }
        }
        return views;
    }

    @Nullable
    @Override
    public List<GenericTableIndex> getIndexes(@NotNull DBRProgressMonitor monitor) throws DBException {
        List<GenericTableIndex> indexes = new ArrayList<>();
        for (CubridTable table : getPhysicalTables(monitor)) {
            indexes.addAll(table.getIndexes(monitor));
        }
        return indexes;
    }

	@Nullable
    public Collection<CubridTrigger> getCubridTriggers(@NotNull DBRProgressMonitor monitor)
            throws DBException {
        return getCubridTriggerCache().getAllObjects(monitor, this);
    }

    public static class GroupListProvider implements IPropertyValueListProvider<CubridUser>
    {
        @Override
        public boolean allowCustomValue() {
            return false;
        }

        @NotNull
        @Override
        public Object[] getPossibleValues(@NotNull CubridUser object) {
            return object.allGroups.toArray();
        }
    }

    public class CubridTableCache extends TableCache
    {
        protected CubridTableCache(@NotNull GenericDataSource dataSource) {
            super(dataSource);
        }

        @NotNull
        @Override
        protected GenericTableColumn fetchChild(
                @NotNull JDBCSession session,
                @NotNull GenericStructContainer owner,
                @NotNull GenericTableBase table,
                @NotNull JDBCResultSet dbResult)
                throws SQLException, DBException {
            String columnName = JDBCUtils.safeGetString(dbResult, "attr_name");
            String dataType = JDBCUtils.safeGetString(dbResult, "data_type");
            boolean autoIncrement = false;
            String tableName = table.isSystem() ? table.getName() : ((CubridDataSource) getDataSource()).getMetaModel().getTableOrViewName(table);
            String sql = "show columns from " + DBUtils.getQuotedIdentifier(getDataSource(), tableName) + " where Field = ?";
            try (JDBCPreparedStatement dbStat = session.prepareStatement(sql)) {
                dbStat.setString(1, columnName);
                try (JDBCResultSet result = dbStat.executeQuery()) {
                    if (result.next()) {
                        dataType = JDBCUtils.safeGetString(result, "Type");
                        autoIncrement = JDBCUtils.safeGetString(result, "Extra").equals("auto_increment");
                    }
                }
            }
            return new CubridTableColumn(table, columnName, dataType, autoIncrement, dbResult);
        }
    }

    public class CubridIndexCache extends JDBCCompositeCache<GenericStructContainer, CubridTable, GenericTableIndex, GenericTableIndexColumn>
    {
        CubridIndexCache(@NotNull TableCache tableCache) {
            super(tableCache, CubridTable.class, JDBCConstants.TABLE_NAME, JDBCConstants.INDEX_NAME);
        }

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull GenericStructContainer owner, @Nullable CubridTable forParent)
                throws SQLException {
            return session.getMetaData().getIndexInfo(null, null, forParent.getUniqueName(), false, true).getSourceStatement();
        }

        @Nullable
        @Override
        protected GenericTableIndex fetchObject(
                @NotNull JDBCSession session,
                @NotNull GenericStructContainer owner,
                @Nullable CubridTable parent,
                @Nullable String indexName,
                @NotNull JDBCResultSet dbResult)
                throws SQLException, DBException {
            boolean isNonUnique = JDBCUtils.safeGetBoolean(dbResult, JDBCConstants.NON_UNIQUE);
            String indexQualifier = JDBCUtils.safeGetStringTrimmed(dbResult, JDBCConstants.INDEX_QUALIFIER);
            long cardinality = JDBCUtils.safeGetLong(dbResult, JDBCConstants.INDEX_CARDINALITY);
            int indexTypeNum = JDBCUtils.safeGetInt(dbResult, JDBCConstants.TYPE);
            String name = indexName;

            DBSIndexType indexType;
            switch (indexTypeNum) {
                case DatabaseMetaData.tableIndexStatistic:
                    return null;
                case DatabaseMetaData.tableIndexClustered:
                    indexType = DBSIndexType.CLUSTERED;
                    break;
                case DatabaseMetaData.tableIndexHashed:
                    indexType = DBSIndexType.HASHED;
                    break;
                case DatabaseMetaData.tableIndexOther:
                    indexType = DBSIndexType.OTHER;
                    break;
                default:
                    indexType = DBSIndexType.UNKNOWN;
                    break;
            }
            if (CommonUtils.isEmpty(name)) {
                name = parent.getName().toUpperCase(Locale.ENGLISH) + "_INDEX";
            }
            return new GenericTableIndex(parent, isNonUnique, indexQualifier,
                    cardinality, name, indexType, true);
        }

        @Nullable
        @Override
        protected GenericTableIndexColumn[] fetchObjectRow(
                @NotNull JDBCSession session,
                @NotNull CubridTable parent,
                @NotNull GenericTableIndex object,
                @NotNull JDBCResultSet dbResult)
                throws SQLException, DBException {
            int ordinalPosition = JDBCUtils.safeGetInt(dbResult, JDBCConstants.ORDINAL_POSITION);
            String columnName = JDBCUtils.safeGetString(dbResult, JDBCConstants.COLUMN_NAME);
            String ascOrDesc = JDBCUtils.safeGetStringTrimmed(dbResult, JDBCConstants.ASC_OR_DESC);

            if (CommonUtils.isEmpty(columnName)) {
                return null;
            }
            GenericTableColumn tableColumn = parent.getAttribute(session.getProgressMonitor(), columnName);
            if (tableColumn == null) {
                return null;
            }
            return new GenericTableIndexColumn[]{new GenericTableIndexColumn(
                    object, tableColumn, ordinalPosition, !"D".equalsIgnoreCase(ascOrDesc))
            };
        }

        @Override
        protected void cacheChildren(
                @NotNull DBRProgressMonitor monitor,
                @Nullable GenericTableIndex object,
                @Nullable List<GenericTableIndexColumn> children) {
            object.setColumns(children);
        }
    }
    
//    static class CubridTriggerCache extends JDBCObjectWithParentCache<GenericStructContainer, GenericTableBase, CubridTrigger> {
//
//    	CubridTriggerCache(TableCache tableCache) {
//            super(tableCache, GenericTableBase.class, "OWNER", "TRIGGER_NAME");
//        }
//
//		@Override
//		protected JDBCStatement prepareObjectsStatement(JDBCSession session, GenericStructContainer owner,
//				GenericTableBase tableBase) throws SQLException {
//			return ((CubridMetaModel) owner.getDataSource().getMetaModel()).prepareCubridTriggersLoadStatement(session, owner, tableBase);
//
//		}
//
//		@Override
//		protected CubridTrigger fetchObject(JDBCSession session, GenericStructContainer owner,
//				GenericTableBase tableBase, String childName, JDBCResultSet resultSet) throws SQLException, DBException {
//			// TODO Auto-generated method stub
//			return ((CubridMetaModel) owner.getDataSource().getMetaModel()).createCubridTriggerImpl(session, owner, tableBase, childName, resultSet);
//		}
//    }
    
    class CubridTriggerCache extends JDBCObjectCache<GenericObjectContainer, CubridTrigger> {

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull GenericObjectContainer container) throws SQLException {
            return ((CubridMetaModel) container.getDataSource().getMetaModel()).prepareCubridTriggersLoadStatement(session, container);
        }

        @Nullable
        @Override
        protected CubridTrigger fetchObject(@NotNull JDBCSession session, @NotNull GenericObjectContainer container, @NotNull JDBCResultSet resultSet) throws SQLException, DBException {
            return ((CubridMetaModel) container.getDataSource().getMetaModel()).createCubridTriggerImpl(session, container, resultSet);
        }
    }
 
}
