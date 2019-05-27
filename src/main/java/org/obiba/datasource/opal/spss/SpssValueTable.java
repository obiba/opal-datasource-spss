/*
 * Copyright (c) 2017 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.obiba.datasource.opal.spss;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.obiba.datasource.opal.spss.support.SpssDatasourceParsingException;
import org.obiba.datasource.opal.spss.support.SpssVariableTypeMapper;
import org.obiba.datasource.opal.spss.support.SpssVariableValueFactory;
import org.obiba.datasource.opal.spss.support.SpssVariableValueSourceFactory;
import org.obiba.magma.*;
import org.obiba.magma.support.AbstractValueTable;
import org.obiba.magma.support.DatasourceParsingException;
import org.obiba.magma.support.VariableEntityBean;
import org.obiba.magma.support.VariableEntityProvider;
import org.obiba.magma.type.DateTimeType;
import org.opendatafoundation.data.spss.SPSSFile;
import org.opendatafoundation.data.spss.SPSSVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.*;

public class SpssValueTable extends AbstractValueTable implements Disposable {

  private final static Logger log = LoggerFactory.getLogger(SpssValueTable.class);

  private final SPSSFile spssFile;

  private final String locale;

  private final String idVariable;

  private int idVariableIndex = 0;

  private Map<String, List<Integer>> entityToVariableIndex = new HashMap<>();

  public SpssValueTable(Datasource datasource, String name, String entityType, String locale, String idVariable, SPSSFile spssFile) {
    super(datasource, name);
    this.spssFile = spssFile;
    this.locale = locale;
    this.idVariable = idVariable == null || idVariable.trim().isEmpty() ? null : idVariable;
    loadMetadata();
    if (this.idVariable != null) {
      for (int i = 0; i < spssFile.getVariableCount(); i++) {
        SPSSVariable var = spssFile.getVariable(i);
        if (var.getName().equals(idVariable)) {
          idVariableIndex = i;
          break;
        }
      }
    }
    setVariableEntityProvider(new SpssVariableEntityProvider(entityType, this.idVariableIndex));
  }

  @Override
  public void initialise() {
    initializeVariableSources();
    super.initialise();
  }

  @Override
  public ValueSet getValueSet(VariableEntity entity) throws NoSuchValueSetException {
    return new SpssValueSet(this, entity, idVariableIndex, spssFile, entityToVariableIndex.get(entity.getIdentifier()), isMultilines());
  }

  @NotNull
  @Override
  public Timestamps getTimestamps() {
    return new Timestamps() {
      @NotNull
      @Override
      public Value getLastUpdate() {
        Date lastModified = new Date(spssFile.file.lastModified());
        return DateTimeType.get().valueOf(lastModified);
      }

      @NotNull
      @Override
      public Value getCreated() {
        // Not currently possible to read a file creation timestamp. Coming in JDK 7 NIO.
        return DateTimeType.get().nullValue();
      }
    };
  }

  public boolean isMultilines() {
    // either detected or configured
    return ((SpssVariableEntityProvider) getVariableEntityProvider()).isMultilines() || getSpssDatasource().isMultilines();
  }

  //
  // Private methods
  //

  public SpssDatasource getSpssDatasource() {
    return (SpssDatasource) super.getDatasource();
  }

  private void initializeVariableSources() {
    addVariableValueSources(new SpssVariableValueSourceFactory(spssFile, getEntityType(), locale, idVariableIndex, entityToVariableIndex, isMultilines() ? getName() : null));
  }

  private void loadMetadata() {
    if (spssFile.isMetadataLoaded) {
      return;
    }

    try {
      spssFile.loadMetadata();
    } catch (Exception e) {
      String fileName = spssFile.file.getName();
      throw new DatasourceParsingException("Failed load meta data in file " + fileName, e, "SpssFailedToLoadMetadata",
          fileName);
    }
  }

  @Override
  public void dispose() {
    if (spssFile != null) {
      try {
        spssFile.close();
      } catch (IOException e) {
        log.warn("Error occurred while closing SPSS file: {}", e.getMessage());
      }
    }
  }

  //
  // Inner Classes
  //

  private class SpssVariableEntityProvider implements VariableEntityProvider {

    @NotNull
    private final String entityType;

    private final int idVariableIndex;

    private List<VariableEntity> variableEntities;

    boolean multilines = false;

    private SpssVariableEntityProvider(@Nullable String entityType, int idVariableIndex) {
      this.entityType = entityType == null || entityType.trim().isEmpty() ? "Participant" : entityType.trim();
      this.idVariableIndex = idVariableIndex;
    }

    @NotNull
    @Override
    public String getEntityType() {
      return entityType;
    }

    @Override
    public boolean isForEntityType(String anEntityType) {
      return getEntityType().equals(anEntityType);
    }

    @NotNull
    @Override
    public List<VariableEntity> getVariableEntities() {
      if (variableEntities == null) {
        loadData();
        variableEntities = getVariableEntitiesInternal();
      }

      return variableEntities;
    }

    private boolean isMultilines() {
      // make sure entities have been scanned
      getVariableEntities();
      return multilines;
    }

    private List<VariableEntity> getVariableEntitiesInternal() {
      Collection<String> entityIdentifiers = new HashSet<>();
      ImmutableList.Builder<VariableEntity> entitiesBuilder = ImmutableList.builder();
      SPSSVariable entityVariable = spssFile.getVariable(idVariableIndex);
      int numberOfObservations = entityVariable.getNumberOfObservations();
      ValueType valueType = SpssVariableTypeMapper.map(entityVariable);

      for (int i = 1; i <= numberOfObservations; i++) {
        Value identifierValue = new SpssVariableValueFactory(Lists.newArrayList(i), entityVariable, valueType, true, false).create();

        if (identifierValue.isNull()) {
          throw new SpssDatasourceParsingException("Empty entity identifier found.", "SpssEmptyIdentifier",
              entityVariable.getName(), i).dataInfo(entityVariable.getName(), i);
        }

        String identifier = identifierValue.getValue().toString();

        if (entityIdentifiers.contains(identifier)) {
          multilines = true;
          entityToVariableIndex.get(identifier).add(i);
        } else {
          entityToVariableIndex.put(identifier, Lists.newArrayList(i));
          entitiesBuilder.add(new VariableEntityBean(entityType, identifier));
          entityIdentifiers.add(identifier);
        }
      }

      return entitiesBuilder.build();
    }

    private void loadData() {
      if (spssFile.isDataLoaded) {
        return;
      }

      try {
        if (!spssFile.isMetadataLoaded) {
          loadMetadata();
        }

        spssFile.loadData();
      } catch (Exception e) {
        String fileName = spssFile.file.getName();
        throw new DatasourceParsingException("Failed load data in file " + fileName, e, "SpssFailedToLoadData", fileName);
      }
    }

  }

}