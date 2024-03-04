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

import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.struct.DBSObject;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CubridCharset implements DBSObject
{
    private String name;
    private CubridDataSource dataSource;
    private List<CubridCollation> collations = new ArrayList<>();

    protected CubridCharset(CubridDataSource dataSource, ResultSet dbResult)
    {
        this.name = JDBCUtils.safeGetString(dbResult, "charset_name");
        this.dataSource = dataSource;
    }

    public CubridDataSource getDataSource()
    {
        return dataSource;
    }

    public void addCollation(CubridCollation collation)
    {
        collations.add(collation);
        Collections.sort(collations, DBUtils.nameComparator());
    }

    public List<CubridCollation> getCollations()
    {
        return collations;
    }

    public CubridCollation getDefaultCollation()
    {
        for (CubridCollation collation : collations) {
            return collation;
        }
        return null;
    }

    public CubridCollation getCollation(String name)
    {
        for (CubridCollation collation : collations) {
            if (collation.getName().equals(name)) {
                return collation;
            }
        }
        return null;
    }

    public String getName()
    {
        return name;
    }

	@Override
	public String getDescription() {
		return null;
	}

	@Override
	public boolean isPersisted() {
		return false;
	}

	@Override
	public DBSObject getParentObject() {
		return null;
	}
}
