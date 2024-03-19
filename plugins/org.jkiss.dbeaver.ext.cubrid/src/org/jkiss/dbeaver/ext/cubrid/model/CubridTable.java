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
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericTable;
import org.jkiss.dbeaver.ext.generic.model.GenericTableIndex;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.IPropertyValueListProvider;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CubridTable extends GenericTable
{
    private CubridUser owner;
    private CubridUser oldOwner;
    private CubridCharset charset;
    private CubridCollation collation;
    private Integer autoIncrement;
    private boolean reuseOID = true;

    public CubridTable(
            @NotNull GenericStructContainer container,
            @Nullable String tableName,
            @Nullable String tableType,
            @Nullable JDBCResultSet dbResult) {
        super(container, tableName, tableType, dbResult);

        String owner_name;
        String collation_name;
        if (dbResult != null) {
            String type = JDBCUtils.safeGetString(dbResult, CubridConstants.IS_SYSTEM_CLASS);
            this.reuseOID = (JDBCUtils.safeGetString(dbResult, CubridConstants.REUSE_OID)).equals("YES");
            owner_name = JDBCUtils.safeGetString(dbResult, CubridConstants.OWNER_NAME);
            collation_name = JDBCUtils.safeGetString(dbResult, CubridConstants.COLLATION);
            autoIncrement = JDBCUtils.safeGetInteger(dbResult, CubridConstants.AUTO_INCREMENT_VAL);
            if (type != null) {
                this.setSystem(type.equals("YES"));
            }
        } else {
            owner_name = getDataSource().getContainer().getConnectionConfiguration().getUserName().toUpperCase();
            collation_name = CubridConstants.DEFAULT_COLLATION;
        }

        for(GenericSchema cubridOwner : this.getDataSource().getSchemaList()){
            if(cubridOwner.getName().equals(owner_name)) {
                this.owner = (CubridUser) cubridOwner;
                this.oldOwner = (CubridUser) cubridOwner;
            }
        }

        for(CubridCharset cbCharset : getDataSource().getCharsets()){
            if(cbCharset.getName().equals(collation_name.split("_")[0])) {
                this.charset = cbCharset;
            }
        }

        ArrayList<CubridCollation> collation_list = new ArrayList<>();
        for(String col : getDataSource().getCollations()) {
            CubridCollation collation = new CubridCollation(col);
            collation_list.add(collation);
        }

        for(CubridCollation cbCollation : collation_list){
            if(cbCollation.getName().equals(collation_name)) {
                this.collation = cbCollation;
            }
        }
    }

    @NotNull
    @Override
    public CubridDataSource getDataSource() {
        return (CubridDataSource) super.getDataSource();
    }

    @NotNull
    public CubridUser getParent() {
        return (CubridUser) super.getContainer();
    }

    @NotNull
    public boolean supportsTrigger() {
        return ((CubridUser) this.getContainer()).supportsTrigger();
    }

    @Nullable
    public Collection<? extends GenericTableIndex> getIndexes(DBRProgressMonitor monitor)
            throws DBException {
        return getParent().getCubridIndexCache().getObjects(monitor, getContainer(), this);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    @Override
    public List<CubridTableColumn> getAttributes(@NotNull DBRProgressMonitor monitor)
            throws DBException {
        return (List<CubridTableColumn>) super.getAttributes(monitor);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    @Override
    public List<CubridTrigger> getTriggers(@NotNull DBRProgressMonitor monitor) throws DBException {
        return (List<CubridTrigger>) super.getTriggers(monitor);
    }

    @Nullable
    @Override
    @Property(viewable = true, editable = true, updatable = true, listProvider = OwnerListProvider.class, order = 2)
    public GenericSchema getSchema() {
        return owner;
    }

    public void setSchema(@NotNull CubridUser owner) {
        this.owner = owner;
    }

    @NotNull
    public CubridUser getOldSchema() {
        return this.oldOwner;
    }

    @NotNull
    public String getUniqueName() {
        if (getDataSource().getSupportMultiSchema()) {
            return this.getSchema().getName() + "." + this.getName();
        } else {
            return this.getName();
        }
    }

    @NotNull
    @Property(viewable = true, editable = true, updatable = true, listProvider = CollationListProvider.class, order = 9)
    public CubridCollation getCollation() {
        return collation;
    }

    public void setCollation(@NotNull CubridCollation collation) {
        this.collation = collation;
    }

    @NotNull
    @Property(viewable = false, editable = true, updatable = true, listProvider = CharsetListProvider.class, order = 8) 
    public CubridCharset getCharset() {
        return charset;
    }

    public void setCharset(@NotNull CubridCharset charset) {
        this.charset = charset;
        this.collation = charset == null ? null : charset.getDefaultCollation();
    }

    @NotNull
    @Property(viewable = true, editable = true, order = 52)
    public boolean isReuseOID() {
        return reuseOID;
    }

    public void setReuseOID(@NotNull boolean reuseOID) {
        this.reuseOID = reuseOID;
    }

    @Nullable
    @Property(viewable = true, editable = true, updatable = true, order = 10)
    public Integer getAutoIncrement() {
        return autoIncrement == null ? 0 : autoIncrement;
    }

    public void setAutoIncrement(@NotNull Integer autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    @NotNull
    @Override
    public String getFullyQualifiedName(DBPEvaluationContext context) {
        if (this.isSystem()) {
            return DBUtils.getFullQualifiedName(getDataSource(), this);
        } else {
            return DBUtils.getFullQualifiedName(getDataSource(), this.getSchema(), this);
        }
    }

    @NotNull
    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        getParent().getCubridIndexCache().clearObjectCache(this);
        return super.refreshObject(monitor);
    }

    public static class OwnerListProvider implements IPropertyValueListProvider<CubridTable>
    {
        @NotNull
        @Override
        public boolean allowCustomValue() {
            return false;
        }

        @NotNull
        @Override
        public Object[] getPossibleValues(@NotNull CubridTable object) {
            return object.getDataSource().getSchemas().toArray();
        }
    }

    public static class CharsetListProvider implements IPropertyValueListProvider<CubridTable>
    {
        @NotNull
        @Override
        public boolean allowCustomValue() {
            return false;
        }

        @NotNull
        @Override
        public Object[] getPossibleValues(@NotNull CubridTable object) {
            return object.getDataSource().getCharsets().toArray();
        }
    }

    public static class CollationListProvider implements IPropertyValueListProvider<CubridTable>
    {
        @NotNull
        @Override
        public boolean allowCustomValue() {
            return false;
        }

        @NotNull
        @Override
        public Object[] getPossibleValues(@NotNull CubridTable object) {
            return object.charset.getCollations().toArray();
        }
    }
}
