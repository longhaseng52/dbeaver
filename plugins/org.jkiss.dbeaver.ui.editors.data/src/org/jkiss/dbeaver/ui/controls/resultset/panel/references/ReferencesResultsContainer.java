/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2019 Serge Rider (serge@jkiss.org)
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
package org.jkiss.dbeaver.ui.controls.resultset.panel.references;

import org.eclipse.swt.widgets.Composite;
import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.model.data.DBDDataFilter;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSDataContainer;
import org.jkiss.dbeaver.ui.controls.resultset.*;

public class ReferencesResultsContainer implements IResultSetContainer {

    private final IResultSetPresentation presentation;
    private DBSDataContainer dataContainer;
    private ResultSetViewer referencesViewer;

    public ReferencesResultsContainer(Composite parent, IResultSetPresentation presentation) {
        this.presentation = presentation;
        this.referencesViewer = new ResultSetViewer(parent, presentation.getController().getSite(), this);
    }

    public IResultSetPresentation getOwnerPresentation() {
        return presentation;
    }

    @Override
    public DBCExecutionContext getExecutionContext() {
        return presentation.getController().getExecutionContext();
    }

    @Override
    public IResultSetController getResultSetController() {
        return referencesViewer;
    }

    @Override
    public DBSDataContainer getDataContainer() {
        return this.dataContainer;
    }

    public void setDataContainer(DBSDataContainer dataContainer) {
        this.dataContainer = dataContainer;
        this.referencesViewer.refresh();
    }

    @Override
    public boolean isReadyToRun() {
        return true;
    }

    @Override
    public void openNewContainer(DBRProgressMonitor monitor, @NotNull DBSDataContainer dataContainer, @NotNull DBDDataFilter newFilter) {

    }

    @Override
    public IResultSetDecorator createResultSetDecorator() {
        return new ReferencesResultsDecorator(this);
    }

}
