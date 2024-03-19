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
import org.jkiss.dbeaver.ext.cubrid.model.meta.CubridMetaModel;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.dpi.DPIContainer;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSDataType;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CubridDataSource extends GenericDataSource
{
    private final CubridMetaModel metaModel;
    private boolean supportMultiSchema;
    private ArrayList<CubridCharset> charsets;
    private Map<String, CubridCollation> collations;

    public CubridDataSource(DBRProgressMonitor monitor, DBPDataSourceContainer container, CubridMetaModel metaModel)
            throws DBException {
        super(monitor, container, metaModel, new CubridSQLDialect());
        this.metaModel = new CubridMetaModel();
    }

    @DPIContainer
    @NotNull
    @Override
    public CubridDataSource getDataSource() {
        return this;
    }

    public List<GenericSchema> getCubridUsers(DBRProgressMonitor monitor) throws DBException {
        return this.getSchemas();
    }

    @Nullable
    @Override
    public GenericTableBase findTable(
            @NotNull DBRProgressMonitor monitor,
            @Nullable String catalogName,
            @Nullable String schemaName,
            @NotNull String tableName)
            throws DBException {
        if (schemaName != null) {
            return this.getSchema(schemaName).getTable(monitor, tableName);
        } else {
            String[] schemas = tableName.split("\\.");
            if (schemas.length > 1) {
                return this.getSchema(schemas[0].toUpperCase()).getTable(monitor, schemas[1]);
            } else {
                for (GenericSchema schema : this.getCubridUsers(monitor)) {
                    GenericTableBase table = schema.getTable(monitor, tableName);
                    if (table != null) {
                        return table;
                    }
                }
            }
        }
        return null;
    }

    @NotNull
    public CubridMetaModel getMetaModel() {
        return metaModel;
    }

    public Collection<CubridCharset> getCharsets() {
        return charsets;
    }

    public CubridCollation getCollation(String name) {
        return collations.get(name);
    }

    @Override
    public Collection<? extends DBSDataType> getDataTypes(DBRProgressMonitor monitor) throws DBException {
        Map<String, DBSDataType> types = new HashMap<>();
        for (DBSDataType dataType : super.getDataTypes(monitor)) {
            types.put(dataType.getName(), dataType);
        }
        return types.values();
    }

    public CubridCharset getCharset(String name) {
        for (CubridCharset charset : charsets) {
            if (charset.getName().equals(name)) {
                return charset;
            }
        }
        return null;
    }

    public ArrayList<String> getCollations() {
        ArrayList<String> collationList = new ArrayList<String>(collations.keySet());
        return collationList;
    }

    public void loadCharsets(DBRProgressMonitor monitor) throws DBException {
        charsets = new ArrayList<>();
        try (JDBCSession session = DBUtils.openMetaSession(monitor, container, "Load charsets")) {
            String sql = "select * from db_charset";
            final JDBCPreparedStatement dbStat = session.prepareStatement(sql);
            JDBCResultSet dbResult = dbStat.executeQuery();
            try {
                while (dbResult.next()) {
                    CubridCharset charset = new CubridCharset(this, dbResult);
                    charsets.add(charset);
	            }
	        } finally {
	             dbStat.close();
	        }
        } catch (SQLException e) {
            throw new DBException("Load charsets failed", e);
        }
        charsets.sort(DBUtils.<CubridCharset>nameComparator());
    }

    public void loadCollations(DBRProgressMonitor monitor) throws DBException {
        collations = new LinkedHashMap<>();
        try (JDBCSession session = DBUtils.openMetaSession(monitor, container, "Load collations")) {
            String sql = "SHOW COLLATION";
            final JDBCPreparedStatement dbStat = session.prepareStatement(sql);
            JDBCResultSet dbResult = dbStat.executeQuery();
            try {
                while (dbResult.next()) {
                    String charsetName = JDBCUtils.safeGetString(dbResult, "charset");
                    CubridCharset charset = getCharset(charsetName);
                    CubridCollation collation = new CubridCollation(charset, dbResult);
                    collations.put(collation.getName(), collation);
                    charset.addCollation(collation);
	            }
            } finally {
                dbStat.close();
            }
        } catch (SQLException e) {
            throw new DBException("Load collations failed", e);
        }
    }

    @Override
    public void initialize(@NotNull DBRProgressMonitor monitor) throws DBException {
        super.initialize(monitor);
        loadCharsets(monitor);
        loadCollations(monitor);
    }

    public boolean getSupportMultiSchema() {
        return this.supportMultiSchema;
    }

    public void setSupportMultiSchema(boolean supportMultiSchema) {
        this.supportMultiSchema = supportMultiSchema;
    }

    @Override
    public boolean splitProceduresAndFunctions() {
        return true;
    }
}
