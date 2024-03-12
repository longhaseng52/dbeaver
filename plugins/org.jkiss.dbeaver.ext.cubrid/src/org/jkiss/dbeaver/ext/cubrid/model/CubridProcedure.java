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

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.*;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.meta.PropertyLength;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CubridProcedure extends GenericProcedure
{
    private List<CubridProcedureParameter> pro_columns;
    private String returnType;

    public CubridProcedure(
            GenericStructContainer container,
            String procedureName,
            String description,
            DBSProcedureType procedureType,
            String target,
            String returnType) {
        super(container, procedureName, description, procedureType, null, true);
        this.returnType = returnType;
    }

    @Override
    @Property(viewable = true, order = 1)
    public String getName() {
        return super.getName();
    }

    @Property(viewable = true, order = 2)
    public GenericSchema getSchema() {
        return super.getSchema();
    }

    @Override
    public GenericCatalog getCatalog() {
        return null;
    }

    @Override
    public GenericFunctionResultType getFunctionResultType() {
        return null;
    }

    @Override
    public GenericPackage getPackage() {
        return null;
    }

    @Property(viewable = true, order = 20)
    public String getReturnType() {
        return returnType;
    }

    @Property(viewable = true, length = PropertyLength.MULTILINE, order = 100)
    public String getDescription() {
        return super.getDescription();
    }

    public void addColumn(CubridProcedureParameter column) {
        if (this.pro_columns == null) {
            this.pro_columns = new ArrayList<>();
        }
        this.pro_columns.add(column);
    }

    public List<CubridProcedureParameter> getParams(DBRProgressMonitor monitor) throws DBException {
        if (pro_columns == null) {
            loadProcedureColumns(monitor);
            if (pro_columns == null) {
                pro_columns = new ArrayList<>();
            }
        }
        return pro_columns;
    }

    @Override
    public String getFullyQualifiedName(DBPEvaluationContext context) {
        return getName();
    }

    @Override
    public void loadProcedureColumns(DBRProgressMonitor monitor) throws DBException {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, getDataSource(), "Read procedure parameter")) {
            String stmt = "select * from db_stored_procedure_args where sp_name = '" + getName() + "'";
            try (JDBCPreparedStatement dbStat = session.prepareStatement(stmt)) {
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    while (dbResult.next()) {
                        String argName = JDBCUtils.safeGetString(dbResult, "arg_name");
                        String dataType = JDBCUtils.safeGetString(dbResult, "data_type");
                        String mode = JDBCUtils.safeGetString(dbResult, "mode");
                        String comment = JDBCUtils.safeGetString(dbResult, "comment");

                        addColumn(new CubridProcedureParameter(getName(), argName, dataType, mode, comment));
                    }
                }
            }
        } catch (SQLException e) {
            throw new DBException(e, getDataSource());
        }
    }
}
