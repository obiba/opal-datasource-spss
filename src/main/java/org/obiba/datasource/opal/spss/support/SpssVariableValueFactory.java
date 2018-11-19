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

import org.obiba.magma.ValueType;
import org.obiba.magma.type.DecimalType;
import org.obiba.magma.type.IntegerType;
import org.opendatafoundation.data.FileFormatInfo;
import org.opendatafoundation.data.spss.SPSSFileException;
import org.opendatafoundation.data.spss.SPSSNumericVariable;
import org.opendatafoundation.data.spss.SPSSVariable;

import java.util.List;

public class SpssVariableValueFactory extends SpssValueFactory {

  public SpssVariableValueFactory(List<Integer> valuesIndex, SPSSVariable spssVariable, ValueType valueType, boolean withValidation, boolean repeatable) {
    super(valuesIndex, spssVariable, valueType, withValidation, repeatable);
  }

  @Override
  protected String getValue(int index) {
    try {
      return SpssVariableValueConverter.getInstance().convert(spssVariable, index);
    } catch(SPSSFileException | SpssValueConversionException e) {
      String variableName = spssVariable.getName();
      throw new SpssDatasourceParsingException("Failed to retieve variable value.", "SpssFailedToCreateVariable",
          variableName, index).dataInfo(variableName, index).extraInfo(e.getMessage());
    }
  }
}
