/*
 * Copyright (c) 2017 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.obiba.datasource.opal.spss.support;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.obiba.datasource.opal.spss.SpssVariableValueSource;
import org.obiba.magma.Attribute;
import org.obiba.magma.Category;
import org.obiba.magma.ValueType;
import org.obiba.magma.Variable;
import org.obiba.magma.VariableValueSource;
import org.obiba.magma.VariableValueSourceFactory;
import org.obiba.magma.type.TextType;
import org.opendatafoundation.data.spss.SPSSFile;
import org.opendatafoundation.data.spss.SPSSFileException;
import org.opendatafoundation.data.spss.SPSSNumericVariable;
import org.opendatafoundation.data.spss.SPSSVariable;
import org.opendatafoundation.data.spss.SPSSVariableCategory;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.obiba.datasource.opal.spss.support.CharacterSetValidator.validate;

public class SpssVariableValueSourceFactory implements VariableValueSourceFactory {

  //
  // Data members
  //
  @NotNull
  private final SPSSFile spssFile;

  @NotNull
  private final String entityType;

  @NotNull
  private final String locale;

  private final int idVariableIndex;

  private final String occurrenceGroup;

  private final Map<String, List<Integer>> identifierToVariableIndex;

  /**
   *
   * @param spssFile
   * @param identifierToVariableIndex
   * @throws IOException
   * @throws SPSSFileException
   */
  public SpssVariableValueSourceFactory(@NotNull SPSSFile spssFile, @NotNull String entityType, @NotNull String locale, int idVariableIndex,
                                        Map<String, List<Integer>> identifierToVariableIndex, String occurrenceGroup) {
    this.spssFile = spssFile;
    this.entityType = entityType;
    this.locale = locale;
    this.idVariableIndex = idVariableIndex;
    this.identifierToVariableIndex = identifierToVariableIndex;
    this.occurrenceGroup = occurrenceGroup;
  }

  @Override
  public Set<VariableValueSource> createSources() {
    Set<VariableValueSource> sources = Sets.newLinkedHashSet();

    for(int i = 0; i < spssFile.getVariableCount(); i++) {
      if (i != idVariableIndex) {
        SPSSVariable spssVariable = spssFile.getVariable(i);
        try {
          sources.add(new SpssVariableValueSource(createVariableBuilder(i, spssVariable), spssVariable,
              identifierToVariableIndex));
        } catch(SpssInvalidCharacterException e) {
          String variableName = spssVariable.getName();
          // In the dictionary the first row is reserved for entity variable
          int variableIndex = i + 1;
          throw new SpssDatasourceParsingException("Failed to create variable.", "InvalidCharsetCharacter",
              variableIndex, e.getSource()).metadataInfo(variableName, variableIndex).extraInfo(e);
        }
      }
    }

    return sources;
  }

  //
  // Private methods
  //

  private void initializeCategories(SPSSVariable variable, Variable.Builder builder) throws SpssInvalidCharacterException {
    if(variable.categoryMap != null) {
      for(String category : variable.categoryMap.keySet()) {
        SPSSVariableCategory spssCategory = variable.categoryMap.get(category);
        validate(category);
        String catName = variable instanceof SPSSNumericVariable ? CharacterSetValidator.normalizeNumberString(category) : category;
        validate(spssCategory.label);
        builder.addCategory(Category.Builder.newCategory(catName)
            .addAttribute(addLabelAttribute(spssCategory.label))
            .missing(isCategoryValueMissingCode(variable, spssCategory)).build());
      }
    }
  }

  private boolean isCategoryValueMissingCode(SPSSVariable spssVariable, SPSSVariableCategory spssCategory) {
    if(spssVariable instanceof SPSSNumericVariable) {
      return spssVariable.isMissingValueCode(spssCategory.value);
    }

    return spssVariable.isMissingValueCode(spssCategory.strValue);
  }

  @SuppressWarnings("ChainOfInstanceofChecks")
  private Variable createVariableBuilder(int variableIndex, @NotNull SPSSVariable spssVariable)
      throws SpssInvalidCharacterException {
    String variableName = spssVariable.getName();
    validate(variableName);
    Variable.Builder builder = Variable.Builder.newVariable(variableName, TextType.get(), entityType);
    builder.index(variableIndex);
    addAttributes(builder, spssVariable);
    addLabel(builder, spssVariable);
    ValueType valueType = SpssVariableTypeMapper.map(spssVariable);
    builder.type(valueType);
    initializeCategories(spssVariable, builder);
    if (!Strings.isNullOrEmpty(occurrenceGroup)) {
      builder.repeatable();
      builder.occurrenceGroup(occurrenceGroup);
    }
    return builder.build();
  }

  private void addLabel(@NotNull Variable.Builder builder, @NotNull SPSSVariable spssVariable)
      throws SpssInvalidCharacterException {
    String label = spssVariable.getLabel();

    if(!Strings.isNullOrEmpty(label)) {
      builder.addAttribute(addLabelAttribute(label));
    }
  }

  private Attribute addLabelAttribute(String value) {
    Attribute.Builder attributeBuilder = Attribute.Builder.newAttribute("label").withValue(value);

    if(!Strings.isNullOrEmpty(locale)) {
      attributeBuilder.withLocale(new Locale(locale));
    }

    return attributeBuilder.build();
  }

  private void addAttributes(Variable.Builder builder, @NotNull SPSSVariable spssVariable)
      throws SpssInvalidCharacterException {
    builder.addAttribute(createAttribute("measure", spssVariable.getMeasureLabel()))
        .addAttribute(createAttribute("width", spssVariable.getLength()))
        .addAttribute(createAttribute("decimals", spssVariable.getDecimals()))
        .addAttribute(createAttribute("shortName", spssVariable.getShortName()))
        .addAttribute(createAttribute("format", spssVariable.getSPSSFormat()));
  }

  private Attribute createAttribute(String attributeName, @Nullable String value) throws SpssInvalidCharacterException {
    validate(value);
    return Attribute.Builder.newAttribute(attributeName).withNamespace("spss").withValue(value).build();
  }

  private Attribute createAttribute(String attributeName, int value) throws SpssInvalidCharacterException {
    return createAttribute(attributeName, String.valueOf(value));
  }
}
