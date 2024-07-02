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
import java.util.List;
import java.util.Map;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.cubrid.CubridConstants;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.ext.generic.model.GenericScriptObject;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBPNamedObject2;
import org.jkiss.dbeaver.model.DBPQualifiedObject;
import org.jkiss.dbeaver.model.DBPSaveableObject;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.IPropertyValueListProvider;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.meta.PropertyLength;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLUtils;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.rdb.DBSTrigger;
import org.jkiss.utils.CommonUtils;

public class CubridTrigger<OWNER extends DBSObject> implements
DBSTrigger,
GenericScriptObject,
DBPNamedObject2,
DBPQualifiedObject, DBPSaveableObject
{	
	@NotNull
    private final OWNER container;
	private String name;
    private String description;
    private String owner;
    private String targetColumn;
    private boolean active;
    private boolean persisted;
    private Double priority;
    private String event;
    private String condition;
    private String actionTime;
    private String actionType;
    private String actionDefinition;
//    private String ddl;
    private Map<Integer, String> events = Map.of(0, "UPDATE", 1, "UPDATE STATEMENT", 2, "DELETE", 3, "DELETE STATEMENT", 4, "INSERT", 5, "INSERT STATEMENT", 8, "COMMIT", 9, "ROLLBACK");
    private Map<Integer, String> actionTimes = Map.of(1, "BEFORE", 2, "AFTER", 3, "DEFERRED");
    private Map<Integer, String> actionTypes = Map.of(1, "INSERT, UPDATE, DELETE, and CALL", 2, "REJECT", 3, "INVALIDATE_TRANSACTION", 4, "PRINT");
    private static List<CubridTableColumn> columnRefs;
    List<String> columnList = new ArrayList<>();

    public CubridTrigger(
            @NotNull GenericTableBase container,
            @NotNull String name,
//            @Nullable String description,
            @NotNull JDBCResultSet dbResult) {
//        super(container, name, description);
    	this.container = (OWNER) container;
        this.name = name;
        this.description = JDBCUtils.safeGetString(dbResult, CubridConstants.COMMENT);
        this.owner = JDBCUtils.safeGetString(dbResult, "owner.name");
        this.active = JDBCUtils.safeGetInteger(dbResult, "status").equals(2);
        this.targetColumn = JDBCUtils.safeGetString(dbResult, "target_attribute");
        this.priority = JDBCUtils.safeGetDouble(dbResult, "priority");
        this.event = this.events.get(JDBCUtils.safeGetInteger(dbResult, "event"));
        this.condition = JDBCUtils.safeGetString(dbResult, "condition");
        this.actionTime = this.actionTimes.get(JDBCUtils.safeGetInteger(dbResult, "action_time"));
        this.actionType = this.actionTypes.get(JDBCUtils.safeGetInteger(dbResult, "action_type"));
        this.actionDefinition = JDBCUtils.safeGetString(dbResult, "action_definition");
        this.persisted = true;
    }

    public CubridTrigger(
            @NotNull GenericTableBase container,
            @NotNull String name,
            DBRProgressMonitor monitor) throws DBException {
//        super(container, name, null);
    	this.container = (OWNER) container;
        this.name = name;
        this.owner = getDataSource().getContainer().getConnectionConfiguration().getUserName().toUpperCase();
        this.active = true;
        this.priority = 0.0;
        this.event = "UPDATE";
        this.actionTime = "BEFORE";
        this.actionType = "INSERT, UPDATE, DELETE, and CALL";
        this.columnRefs = (List<CubridTableColumn>) container.getAttributes(monitor);
        this.persisted = false;
        for (CubridTableColumn col : CommonUtils.safeCollection(columnRefs)) {
            columnList.add(col.getName());
        }
    }

    @NotNull
    @Override
    @Property(viewable = true, editable = true, order = 1)
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @NotNull
    @Override
    public CubridDataSource getDataSource()
    {
        return (CubridDataSource) container.getDataSource();
    }

    @Override
    public OWNER getParentObject()
    {
        return container;
    }
    @NotNull
    @Property(viewable = true, editable = true, updatable = true, listProvider = OwnerListProvider.class, order = 2)
    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
    	this.owner = owner;
    }
    
    @Property(viewable = true, editable = true, listProvider = ColumnNameListProvider.class, order = 3)
    public String getTargetColumn() {
        return targetColumn;
    }

    public void setTargetColumn(String targetColumn) {
        this.targetColumn = targetColumn;
    }

//    @Nullable
//    @Override
//    @Property(viewable = true, order = 4)
//    public CubridTable getTable() {
//        return container;
//    }

    @Property(viewable = true, editable = true, updatable = true, order = 5)
    public boolean getActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    @NotNull
    @Property(viewable = true, editable = true, updatable = true, order = 10)
    public Double getPriority() {
        return priority;
    }

    public void setPriority(Double priority) {
        this.priority = priority;
    }

    @Nullable
    @Property(viewable = true, editable = true, listProvider = ActionTimeListProvider.class, order = 20)
    public String getActionTime() {
        return actionTime;
    }

    public void setActionTime(String actionTime) {
        this.actionTime = actionTime;
    }

    @Nullable
    @Property(viewable = true, editable = true, listProvider = EventOptionListProvider.class, order = 30)
    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    @NotNull
    @Property(viewable = true, editable = true, order = 40)
    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    @Nullable
    @Property(viewable = true, editable = true, listProvider = ActionTypeListProvider.class, order = 50)
    public String getActionType() {
        return actionType;
    }

    public void setActionType(String actionType) {
        this.actionType = actionType;
    }

    @NotNull
    @Property(viewable = true, editable = true, order = 60)
    public String getActionDefinition() {
        return actionDefinition;
    }

    public void setActionDefinition(String actionDefinition) {
        this.actionDefinition = actionDefinition;
    }

    @Nullable
    @Override
    @Property(viewable = true, editable = true, updatable = true, length = PropertyLength.MULTILINE, order = 100)
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @NotNull
    @Override
    public String getFullyQualifiedName(@NotNull DBPEvaluationContext context) {
//        return DBUtils.getFullQualifiedName(getDataSource(), this);
        if (this.getDataSource().getSupportMultiSchema()) {
        	return DBUtils.getFullQualifiedName(getDataSource(), getTable().getSchema(), this);
        } else {
        	return DBUtils.getFullQualifiedName(getDataSource(), this);
        }
    }

    public static class OwnerListProvider implements IPropertyValueListProvider<CubridTrigger>
    {
        @NotNull
        @Override
        public boolean allowCustomValue() {
            return false;
        }

        @NotNull
        @Override
        public Object[] getPossibleValues(@NotNull CubridTrigger object) {
        	List<String> schemas = new ArrayList<>();
        	for (GenericSchema schema : object.getDataSource().getSchemas()) {
                schemas.add(schema.getName());
            }
            return schemas.toArray();
        }
    }

    public static class ColumnNameListProvider implements IPropertyValueListProvider<CubridTrigger> {

        @Override
        public boolean allowCustomValue() {
            return false;
        }

        @Override
        public Object[] getPossibleValues(CubridTrigger object) {
            return object.columnList.toArray();
        }
    }

    public static class EventOptionListProvider implements IPropertyValueListProvider<CubridTrigger> {

        @Override
        public boolean allowCustomValue() {
            return false;
        }

        @Override
        public Object[] getPossibleValues(CubridTrigger object) {
            return CubridConstants.EVENT_OPTION;
        }
    }

    public static class ActionTimeListProvider implements IPropertyValueListProvider<CubridTrigger> {

        @Override
        public boolean allowCustomValue() {
            return false;
        }

        @Override
        public Object[] getPossibleValues(CubridTrigger object) {
            return CubridConstants.ACTION_TIME_OPTION;
        }
    }

    public static class ActionTypeListProvider implements IPropertyValueListProvider<CubridTrigger> {

        @Override
        public boolean allowCustomValue() {
            return false;
        }

        @Override
        public Object[] getPossibleValues(CubridTrigger object) {
            return CubridConstants.ACTION_TYPE_OPTION;
        }
    }

    @Override
    @Property(viewable = true, order = 4)
    public GenericTableBase getTable() {
        return (GenericTableBase) getParentObject();
    }

	@Override
	public boolean isPersisted() {
		// TODO Auto-generated method stub
		return persisted;
	}

//	public void setObjectDefinitionText(String ddl) {
//		this.ddl = ddl;
//	}
	@Override
	public String getObjectDefinitionText(DBRProgressMonitor monitor, Map<String, Object> options) throws DBException {
		if (persisted) {
			StringBuilder ddl = new StringBuilder();
			ddl.append("CREATE TRIGGER ");
	    	ddl.append(getFullyQualifiedName(DBPEvaluationContext.DDL));
	        ddl.append(getActive() ? "\nSTATUS ACTIVE" : "\nSTATUS INACTIVE");
	        ddl.append("\nPRIORITY ").append(getPriority());
	        ddl.append("\n" + getActionTime());
	        ddl.append(" ");
	        if (getEvent().equals("COMMIT") || getEvent().equals("ROLLBACK")) {
	            ddl.append(getEvent());
	        } else {
	            ddl.append(getEvent());
	            ddl.append(" ON ").append(getTable());
	            if (getEvent().equals("UPDATE") || getEvent().equals("UPDATE STATEMENT")) {
	                if (getTargetColumn() != null) {
	                    ddl.append("(" + getTargetColumn() + ")");
	                }
	            }
	            if (condition != null) {
	                ddl.append("\nIF ").append(getCondition());
	            }
	        }
	
	        ddl.append("\nEXECUTE ");
	        if (getActionType().equals("REJECT") || getActionType().equals("INVALIDATE_TRANSACTION")) {
	            ddl.append(getActionType());
	        } else if (getActionType().equals("PRINT")) {
	            ddl.append(getActionType() + " ");
	            ddl.append(getActionDefinition() == null ? "" : SQLUtils.quoteString(getDataSource(), actionDefinition));
	        }
	        else {
	            ddl.append(getActionDefinition() == null ? "" : actionDefinition);
	        }
	        return ddl.toString();
		}	
		return "-- Trigger definition not available";
	}

	@Override
	public void setPersisted(boolean persisted) {
		this.persisted = persisted;
	}
}
