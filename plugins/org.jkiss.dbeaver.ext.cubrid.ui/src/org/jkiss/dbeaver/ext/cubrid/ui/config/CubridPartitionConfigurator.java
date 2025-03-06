package org.jkiss.dbeaver.ext.cubrid.ui.config;

import java.util.Map;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.ext.cubrid.model.CubridPartition;
import org.jkiss.dbeaver.model.edit.DBECommandContext;
import org.jkiss.dbeaver.model.edit.DBEObjectConfigurator;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.runtime.DBWorkbench;
import org.jkiss.dbeaver.ui.UITask;

public class CubridPartitionConfigurator implements DBEObjectConfigurator<CubridPartition> {

    @Override
    public CubridPartition configureObject(
            @NotNull DBRProgressMonitor monitor,
            @Nullable DBECommandContext commandContext,
            @Nullable Object parent,
            @NotNull CubridPartition newPartition,
            @NotNull Map<String, Object> options) {
        return UITask.run(() -> {
//        	if (!DBWorkbench.getPlatformUI().confirmAction(
//                    "Partition Warning",
//                    "When changing the table type, data is physically moved between tables, so it takes time to change depending on the amount of stored records. Do you want to continue?")) {
//                return null;
//            }
        	CreateCubridPartitionPage editPage = new CreateCubridPartitionPage(monitor, newPartition);
            if (!editPage.edit()) {
                return null;
            }
            newPartition.setName(editPage.getName());
            newPartition.setTableType(editPage.getType());
            newPartition.setPartitionKey(editPage.getPartitionKey());
            newPartition.setPartitionValues(editPage.getPartitionValues());
            newPartition.setDescription(editPage.getDescription());
            return newPartition;
        });
    }
}
