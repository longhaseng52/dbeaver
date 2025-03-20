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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.ext.cubrid.model.CubridUser.CubridTableCache;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.meta.PropertyLength;
import org.jkiss.dbeaver.model.struct.rdb.DBSTablePartition;

public class CubridPartition extends CubridTable implements DBSTablePartition {

    private CubridTable table;
    private String partitionType;
    private String partitionKey;
    private String partitionValues;
    private String description;

    public CubridPartition(
        @NotNull CubridTable table,
        @NotNull String name,
        @NotNull String type,
        @Nullable JDBCResultSet dbResult) {
        super(table.getContainer(), name, type, dbResult);
        this.table = table;
        if (dbResult != null) {
            this.partitionKey = JDBCUtils.safeGetString(dbResult, "partition_expr").replace("[", "").replace("]", "");
            if ("RANGE".equals(type)) {
                Object[] partitions = (Object[]) JDBCUtils.safeGetObject(dbResult, "partition_values");
                this.partitionValues = partitions[1] == null ? "MAXVALUE" : partitions[1].toString();
            } else {
                this.partitionValues = Arrays.toString((Object[]) JDBCUtils.safeGetObject(dbResult, "partition_values"));
            }
            this.description = JDBCUtils.safeGetString(dbResult, "comment");
        }
    }

    @Override
    @Property(viewable = true, editable = true, order = 1)
    public String getName() {
        return super.getName();
    }

    @Override
    public CubridTable getParentTable() {
        return table;
    }

    @Override 
    public boolean isSubPartition(){
        return false;
    }

    @Override 
    public DBSTablePartition getPartitionParent() {
        return null;
    }
    
    @NotNull
    @Override
    @Property(viewable = true, order = 2)
    public String getTableType() {
        return partitionType != null ? partitionType : super.getTableType();
    }
    
    public void setTableType(String type) {
        this.partitionType = type;
    }

    @Property(viewable = true, order = 5)
    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    @Property(viewable = true, editable = true, order = 6)
    public String getPartitionValues() {
        return partitionValues;
    }

    public void setPartitionValues(String partitionValues) {
        this.partitionValues = partitionValues;
    }

    @Property(viewable = true, editable = true, length = PropertyLength.MULTILINE, order = 100)
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    public List<CubridPartition> getPartitionsFromTableCache(CubridPartition partition) {
        List<CubridPartition> partitions = new ArrayList<>();
        CubridTableCache tableCache = (CubridTableCache) getParentTable().getContainer().getTableCache();
        CubridTable partitionParent = partition.getParentTable();
        for (GenericTableBase table : tableCache.getCachedObjects()) {
            if (table instanceof CubridPartition part && partitionParent == ((CubridPartition) table).getParentTable()) {
                partitions.add(part);
            }
        }
        return partitions;
    }

    // Hidden Properties
    @Override
    @Property(hidden = true)
    public boolean isPartitioned() {
        return super.isPartitioned();
    }

    @Override
    @Property(hidden = true)
    public GenericSchema getSchema() {
        return super.getSchema();
    }

    @NotNull
    @Override
    @Property(hidden = true)
    public CubridCollation getCollation() {
        return super.getCollation();
    }

    @NotNull
    @Override
    @Property(hidden = true)
    public boolean isReuseOID() {
        return super.isReuseOID();
    }

    @Nullable
    @Override
    @Property(hidden = true)
    public Integer getAutoIncrement() {
        return super.getAutoIncrement();
    }

    @NotNull
    @Override
    @Property(hidden = true) 
    public CubridCharset getCharset() {
        return super.getCharset();
    }

}
