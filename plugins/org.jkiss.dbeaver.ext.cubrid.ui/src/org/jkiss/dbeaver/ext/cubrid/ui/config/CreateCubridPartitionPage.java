/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2025 DBeaver Corp and others
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
package org.jkiss.dbeaver.ext.cubrid.ui.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Text;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.cubrid.CubridConstants;
import org.jkiss.dbeaver.ext.cubrid.model.CubridPartition;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTable.PartitionCache;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTableColumn;
import org.jkiss.dbeaver.ext.cubrid.ui.internal.CubridMessages;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.runtime.DBWorkbench;
import org.jkiss.dbeaver.ui.UIUtils;
import org.jkiss.dbeaver.ui.editors.object.struct.BaseObjectEditPage;
import org.jkiss.utils.CommonUtils;

public class CreateCubridPartitionPage extends BaseObjectEditPage {

    private String partitionName;
    private CubridPartition partition;
    private String partitionType = "RANGE";
    private String partitionKey;
    private List<CubridTableColumn> columns = new ArrayList<>();
    private DBRProgressMonitor monitor;
    private String partitionValue;
    private String description;

    public CreateCubridPartitionPage(DBRProgressMonitor monitor, CubridPartition partition) {
        super(CubridMessages.dialog_struct_partition_title);
        this.partition = partition;
        this.monitor = monitor;
    }

    @Override
    public CubridPartition getObject() {
        return partition;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getPartitionType() {
        return partitionType;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public String getPartitionValues() {
        return partitionValue;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean isPageComplete() {
        return !(CommonUtils.isEmpty(partitionName) || CommonUtils.isEmpty(partitionKey) || CommonUtils.isEmpty(partitionValue));
    }

    @Override
    public Control createPageContents(Composite parent) {
        Composite propsGroup = new Composite(parent, SWT.NONE);
        propsGroup.setLayout(new GridLayout(2, false));
        propsGroup.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

        //Get List of New Partitions from Table Cache
        List<CubridPartition> partitions = partition.getPartitionsFromTableCache(partition);
        CubridPartition newPartition = (!CommonUtils.isEmpty(partitions)) ? partitions.get(0) : null;

        //Get List of Existing Partitions from Partition Cache
        PartitionCache partitionCache = partition.getParentTable().getPartitionCache();
        CubridPartition oldPartition = !partitionCache.getCachedObjects().isEmpty() ? partitionCache.getCachedObjects().get(0) : null;

        CubridPartition partition = oldPartition != null ? oldPartition : newPartition;
        int length = partitions.size() + partitionCache.getCacheSize();
        if (partition != null) {
            createExistingPartitionFields(propsGroup, partition, length);
        } else {
            createNewPartitionFields(propsGroup);
        }
        return propsGroup;
    }

    private void createExistingPartitionFields(Composite propsGroup, CubridPartition partition, int length) {
        // Partition Type (Read-Only)
    	Combo typeCombo = UIUtils.createLabelCombo(propsGroup, "Partition Type", SWT.DROP_DOWN | SWT.READ_ONLY);
        partitionType = partition.getTableType();
        typeCombo.add(partitionType);
        typeCombo.setText(partition.getTableType());
        typeCombo.setEnabled(false);

        // Partition Key (Read-Only)
        Combo keyCombo = UIUtils.createLabelCombo(propsGroup, "Partition Key", SWT.DROP_DOWN | SWT.READ_ONLY);
        partitionKey = partition.getPartitionKey().replace("[", "").replace("]", "");
        keyCombo.add(partitionKey);
        keyCombo.setText(partitionKey);
        keyCombo.setEnabled(false);

        // Partition Name
        final Text nameText = UIUtils.createLabelText(propsGroup, "Partition Name", null);
        boolean isHash = "HASH".equals(partitionType);
        partitionName = isHash ? "p" + length : nameText.getText().trim();
        nameText.setEnabled(!isHash);
        nameText.addModifyListener(e -> {
            partitionName = nameText.getText().trim();
            updatePageState();
        });

        // Partition Value
        final Text valueText = UIUtils.createLabelText(propsGroup, "Partition Value", null);
        valueText.addModifyListener(e -> {
            partitionValue = valueText.getText().trim();
            updatePageState();
        });

        // Description
        final Text descText = UIUtils.createLabelText(propsGroup, "Description", null);
        descText.setEnabled(!isHash);
        descText.addModifyListener(e -> {
            description = descText.getText().trim();
            updatePageState();
        });
    }

    private void createNewPartitionFields(Composite propsGroup) {
        // Partition Type
    	Combo typeCombo = UIUtils.createLabelCombo(propsGroup, "Partition Type", SWT.DROP_DOWN | SWT.READ_ONLY);
        Arrays.asList("RANGE", "LIST", "HASH").forEach(typeCombo::add);
        typeCombo.setText(partitionType);

        // Partition Key
        Combo keyCombo = UIUtils.createLabelCombo(propsGroup, "Partition Key", SWT.DROP_DOWN | SWT.READ_ONLY);
        try {
            columns = (List<CubridTableColumn>) this.partition.getParentTable().getAttributes(monitor);
            columns.forEach(col -> keyCombo.add(col.getName()));
        } catch (DBException e) {
            DBWorkbench.getPlatformUI().showError(
                CubridMessages.error_loading_columns_title,
                CubridMessages.error_loading_columns_message, e);
            return;
        }
        keyCombo.addModifyListener(e -> {
            partitionKey = keyCombo.getText().trim();
            CubridTableColumn column = columns.stream().filter(col -> col.getName().equals(partitionKey)).findFirst().orElse(null);
            if (column != null && !Arrays.asList(CubridConstants.PARTITION_KEY_SUPPORT).contains(column.getTypeName())) {
                DBWorkbench.getPlatformUI().showWarningMessageBox(
                    CubridMessages.select_partition_range_key_warning_title,
                    CubridMessages.select_partition_range_key_warning_message);
            }
            updatePageState();
        });

        // Partition Name
        final Text nameText = UIUtils.createLabelText(propsGroup, "Partition Name", null);
        nameText.addModifyListener(e -> {
            partitionName = nameText.getText().trim();
            updatePageState();
        });

        // Partition Key
        final Text valueText = UIUtils.createLabelText(propsGroup, "Partition Value", null);
        valueText.addModifyListener(e -> {
            partitionValue = valueText.getText().trim();
            updatePageState();
        });

        // Description
        final Text descText = UIUtils.createLabelText(propsGroup, "Description", null);
        descText.addModifyListener(e -> {
            description = descText.getText().trim();
            updatePageState();
        });

        typeCombo.addModifyListener(e -> {
            partitionType = typeCombo.getText().trim();
            boolean isHash = "HASH".equals(partitionType);
            partitionName = "p0";
            nameText.setEnabled(!isHash);
            descText.setEnabled(!isHash);
            updatePageState();
        });
    }
}
