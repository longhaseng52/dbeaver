package org.jkiss.dbeaver.ext.cubrid.edit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.cubrid.model.CubridPartition;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTable;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTable.PartitionCache;
import org.jkiss.dbeaver.ext.cubrid.model.CubridUser.CubridTableCache;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.model.edit.DBECommandContext;
import org.jkiss.dbeaver.model.edit.DBEPersistAction;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.impl.edit.SQLDatabasePersistAction;
import org.jkiss.dbeaver.model.messages.ModelMessages;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLUtils;
import org.jkiss.utils.CommonUtils;

public class CubridPartitionManager extends CubridTableManager {

    @Override
    protected CubridPartition createDatabaseObject(
            @NotNull DBRProgressMonitor monitor,
            @NotNull DBECommandContext context,
            Object container,
            Object copyFrom,
            @NotNull Map<String, Object> options) {
        return new CubridPartition((CubridTable) container, "partion_name", "", null);
    }

    @Override
    public boolean canCreateObject(@NotNull Object container) {
        return true;
    }

    @Override
    protected void addStructObjectCreateActions(
            DBRProgressMonitor monitor,
            DBCExecutionContext executionContext,
            List<DBEPersistAction> actions,
            StructCreateCommand command,
            Map<String, Object> options) throws DBException {
    	CubridPartition currentPartition = (CubridPartition) command.getObject();
    	CubridTableCache tableCache = (CubridTableCache) this.getObjectsCache(currentPartition);
        List<GenericTableBase> tables = tableCache.getCachedObjects();

        PartitionCache partitionCache = currentPartition.getParentTable().getPartitionCache();
        int cachePartitionsSize = partitionCache.getCachedObjects().size();
        CubridPartition partitionFromCache = !partitionCache.getCachedObjects().isEmpty() ? partitionCache.getCachedObjects().get(0) : null;

        CubridTable partitionParent = currentPartition.getParentTable();
        List<CubridPartition> partitions = new ArrayList<>();
        for (GenericTableBase table : tables) {
            if (table instanceof CubridPartition) {
            	if (partitionParent == ((CubridPartition) table).getParentTable()) {
            	    partitions.add((CubridPartition) table);
            	}
            }
        }
        partitions = partitions.subList(cachePartitionsSize, partitions.size());

        CubridPartition firstPartition = partitions.get(0);
    	String type = currentPartition.getTableType();
        StringBuilder query = new StringBuilder();

        query.append("ALTER TABLE ").append(currentPartition.getParent()).append(".").append(currentPartition.getParentTable().getName());
        boolean isPartitioned = currentPartition.getParentTable().isPartitioned();
        if (isPartitioned) {
            query.append(" ADD PARTITION (");
        } else {
        	query.append(" PARTITION BY ").append(type).append(" (").append(currentPartition.getPartitionKey()).append(") (");
        }

        if ("HASH".equals(type)) {
        	query.deleteCharAt(query.length() - 1);
            query.append("PARTITIONS ").append(currentPartition.getPartitionValues());
	        actions.add( 0, new SQLDatabasePersistAction(ModelMessages.model_jdbc_create_new_table, query.toString()) );

        }
        else {
            if (firstPartition == currentPartition || partitionFromCache == currentPartition) {
	        switch (type) {
	            case "RANGE":
                    sortedPartition(partitions);
	            	for (CubridPartition partition : partitions) {
	            		query.append("\n\tPARTITION ").append(partition.getName()).append(" VALUES LESS THAN ")
	            		.append("MAXVALUE".equals(partition.getPartitionValues()) ? "MAXVALUE" : "(" + partition.getPartitionValues() + ")")
	                    .append(partition.getDescription() == null ? "" : 
	            	        " COMMENT " + SQLUtils.quoteString(partition, CommonUtils.notEmpty(partition.getDescription())))
	            	    .append(",");
	            	}
	            	query.deleteCharAt(query.length() - 1);
	            	query.append("\n)");
	                break;
	            case "LIST":
	            	for (CubridPartition partition : partitions) {
		                query.append("\n\tPARTITION ").append(partition.getName()).append(" VALUES IN ")
		                .append("(" + "'" + String.join("', '", partition.getPartitionValues().split("\\s*,\\s*")) + "'" + ")")
		                .append(partition.getDescription() == null ? "" : 
	            	        " COMMENT " + SQLUtils.quoteString(partition, CommonUtils.notEmpty(partition.getDescription())))
	            	    .append(",");
	            	}
	            	query.deleteCharAt(query.length() - 1);
	            	query.append("\n)");
	                break;
	            case "HASH":
	            	query.deleteCharAt(query.length() - 1);
	                query.append("PARTITIONS ").append(currentPartition.getPartitionValues());
	                break;
	            }

	            actions.add( 0, new SQLDatabasePersistAction(ModelMessages.model_jdbc_create_new_table, query.toString()) );
            }
        }
    }

    public void sortedPartition(List<CubridPartition> partitions) {
        Collections.sort(partitions, new Comparator<CubridPartition>() {
            @Override
            public int compare(CubridPartition p1, CubridPartition p2) {
            	String value1 = p1.getPartitionValues();
                String value2 = p2.getPartitionValues();

                boolean isP1Numeric = isNumeric(value1);
                boolean isP2Numeric = isNumeric(value2);
                
                if (isP1Numeric && isP2Numeric) {
                    return Integer.compare(Integer.parseInt(value1), Integer.parseInt(value2));
                }

                if (isP1Numeric) {
                    return -1;
                } else if (isP2Numeric) {
                    return 1;
                }

                return value1.compareTo(value2);
            }
            
        });
    }
    
    private static boolean isNumeric(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    protected void addObjectDeleteActions(
            @NotNull DBRProgressMonitor monitor,
            @NotNull DBCExecutionContext executionContext,
            @NotNull List<DBEPersistAction> actions,
            @NotNull ObjectDeleteCommand command,
            @NotNull Map<String, Object> options) {
        CubridPartition partition = (CubridPartition) command.getObject();
        actions.add(new SQLDatabasePersistAction("Drop Partition",
                "ALTER TABLE " + partition.getParent().getName() + "." + partition.getParentTable().getName()
                + " DROP PARTITION " + partition.getName()));
    }

    @Override
    protected void appendTableModifiers(
            @NotNull DBRProgressMonitor monitor,
            @NotNull GenericTableBase genericTable,
            @NotNull NestedObjectCommand command,
            @NotNull StringBuilder query,
            @NotNull boolean alter) {
    }
}
