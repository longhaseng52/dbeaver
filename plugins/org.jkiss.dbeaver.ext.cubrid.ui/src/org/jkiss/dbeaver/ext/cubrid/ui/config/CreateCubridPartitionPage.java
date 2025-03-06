package org.jkiss.dbeaver.ext.cubrid.ui.config;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.cubrid.model.CubridPartition;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTable;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTable.PartitionCache;
import org.jkiss.dbeaver.ext.cubrid.model.CubridTableColumn;
import org.jkiss.dbeaver.ext.cubrid.model.CubridUser.CubridTableCache;
import org.jkiss.dbeaver.ext.cubrid.ui.internal.CubridMessages;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.model.DBPDataKind;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.ui.UIUtils;
import org.jkiss.dbeaver.ui.editors.object.struct.BaseObjectEditPage;
import org.jkiss.utils.CommonUtils;

public class CreateCubridPartitionPage extends BaseObjectEditPage {

    private String name;
    private CubridPartition partition;
    private String type = "RANGE";
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
    public DBSObject getObject() {
        return null;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
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
        return !CommonUtils.isEmpty(name) || !CommonUtils.isEmpty(partitionKey) || !CommonUtils.isEmpty(partitionValue);
    }

    @Override
    public Control createPageContents(Composite parent) {
        Composite propsGroup = new Composite(parent, SWT.NONE);
        propsGroup.setLayout(new GridLayout(2, false));
        GridData gd = new GridData(GridData.FILL_HORIZONTAL);
        propsGroup.setLayoutData(gd);
        CubridTable partitionParent = partition.getParentTable();

        CubridTableCache tableCache = (CubridTableCache) partition.getParentTable().getContainer().getTableCache();
        List<GenericTableBase> tables = tableCache.getCachedObjects();
        List<CubridPartition> partitions = new ArrayList<>();
        for (GenericTableBase table : tables) {
            if (table instanceof CubridPartition) {
            	if (partitionParent == ((CubridPartition) table).getParentTable()) {
            	    partitions.add((CubridPartition) table);
            	}
            }
        }
        CubridPartition partitionFromTableCache = (partitions != null && !partitions.isEmpty()) ? partitions.get(0) : null;

        PartitionCache partitionCache = partition.getParentTable().getPartitionCache();
        CubridPartition partitionFromCache = !partitionCache.getCachedObjects().isEmpty() ? partitionCache.getCachedObjects().get(0) : null;
	        if (partitionFromCache != null || partitionFromTableCache != null) {
	        	CubridPartition partition = partitionFromCache != null ? partitionFromCache : partitionFromTableCache;
                createExistingPartitionFields(propsGroup, partition);
	        } else {
	        	createNewPartitionFields(propsGroup);
	        }
            
        return propsGroup;
    }

    private void createExistingPartitionFields(Composite propsGroup, CubridPartition partition) {
    	Combo typeCombo = UIUtils.createLabelCombo(propsGroup, "Partition Type", SWT.DROP_DOWN | SWT.READ_ONLY);
        typeCombo.add(partition.getTableType());
        typeCombo.setText(partition.getTableType());
        this.type = partition.getTableType();
        typeCombo.setEnabled(false);

        Combo targetCombo = UIUtils.createLabelCombo(propsGroup, "Partition Key", SWT.DROP_DOWN | SWT.READ_ONLY);
        this.partitionKey = partition.getPartitionKey().replace("[", "").replace("]", "");
        targetCombo.add(partitionKey);
        targetCombo.setText(partitionKey);
        targetCombo.setEnabled(false);


        final Text nameText = UIUtils.createLabelText(propsGroup, "Partition Name", null);
        
        nameText.addModifyListener(e -> {
        	name = nameText.getText().trim();
            updatePageState();
        });
        boolean isHash = type.equals("HASH");
        name = isHash ? "partition_name" : nameText.getText().trim();
        nameText.setEnabled(!isHash);

        final Text expressionText = UIUtils.createLabelText(propsGroup, "Partition Value", null);
        expressionText.addModifyListener(e -> {
            partitionValue = expressionText.getText().trim();
            updatePageState();
        });

        if (!"HASH".equals(type)) {
	        final Text descText = UIUtils.createLabelText(propsGroup, "Description", null);
	        descText.addModifyListener(e -> {
	            description = descText.getText().trim();
	            updatePageState();
	        });
        }

    }

    private void createNewPartitionFields(Composite propsGroup) {
    	Combo typeCombo = UIUtils.createLabelCombo(propsGroup, "Partition Type", SWT.DROP_DOWN | SWT.READ_ONLY);
        typeCombo.add("RANGE");
        typeCombo.add("LIST");
        typeCombo.add("HASH");
        typeCombo.setText(type);

        Combo targetCombo = UIUtils.createLabelCombo(propsGroup, "Partition Key", SWT.DROP_DOWN | SWT.READ_ONLY);
        try {
            columns = (List<CubridTableColumn>) this.partition.getParentTable().getAttributes(monitor);
            for(CubridTableColumn column : columns) {
                targetCombo.add(column.getName());
            }
        } catch (DBException e1) {
            e1.printStackTrace();
        }

        targetCombo.addModifyListener(e -> {
            partitionKey = targetCombo.getText().trim();

            CubridTableColumn column = null;
            for (CubridTableColumn col : columns) {
                if (col.getName().equals(partitionKey)) {
                    column = col;
                    break;
                }
            }
            if (type.equals("RANGE") && column.getDataKind() != DBPDataKind.NUMERIC) {
                MessageDialog.openWarning(
                        new Shell(),
                        "Partition Warning",
                        "You cannot create a partition range with a non-numeric partition key.\nPlease select a different partition key."
                    );
            } else if (type.equals("LIST") && column.getDataKind() != DBPDataKind.STRING) {
                MessageDialog.openWarning(
                        new Shell(),
                        "Partition Warning",
                        "You cannot create a partition list with a non-string partition key.\nPlease select a different partition key."
                    );
            }
            updatePageState();
        });

        final Text nameText = UIUtils.createLabelText(propsGroup, "Partition Name", null);
        nameText.addModifyListener(e -> {
            name = nameText.getText().trim();
            updatePageState();
        });

        final Text expressionText = UIUtils.createLabelText(propsGroup, "Partition Value", null);
        expressionText.addModifyListener(e -> {
            partitionValue = expressionText.getText().trim();
            updatePageState();
        });

        final Text descText = UIUtils.createLabelText(propsGroup, "Description", null);
        descText.addModifyListener(e -> {
            description = descText.getText().trim();
            updatePageState();
        });
    
        typeCombo.addModifyListener(e -> {
            type = typeCombo.getText().trim();
            boolean isHash = type.equals("HASH");
            name = "partition_name";
            nameText.setEnabled(!isHash);
            descText.setEnabled(!isHash);
            updatePageState();
        });
    }
}
