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
import org.opendatafoundation.data.FileFormatInfo;
import org.opendatafoundation.data.spss.SPSSFileException;
import org.opendatafoundation.data.spss.SPSSNumericVariable;
import org.opendatafoundation.data.spss.SPSSStringVariable;
import org.opendatafoundation.data.spss.SPSSVariable;

public class SpssVariableValueConverter {

  private SpssVariableValueConverter() {}

  static String convert(SPSSVariable spssVariable, int index) throws SpssValueConversionException, SPSSFileException {
    if (spssVariable instanceof SPSSNumericVariable) {
      SPSSNumericVariable spssNumVariable = (SPSSNumericVariable) spssVariable;
      SpssNumericDataType spssNumericDataType = SpssVariableTypeMapper.getSpssNumericDataType(spssNumVariable);
      switch (spssNumericDataType) {
        case COMMA: // comma
        case DOLLAR: // dollar
        case DOT: // dot
        case FIXED: // fixed format (default)
        case SCIENTIFIC:
          Double doubleValue = spssNumVariable.getValue(index);
          return doubleValue.isNaN() ? "" : "" + doubleValue;
      }

    }

    return convert(spssVariable, spssVariable.getValueAsString(index, new FileFormatInfo(FileFormatInfo.Format.ASCII)));
  }


  static String convert(SPSSVariable spssVariable, String value) throws SpssValueConversionException {
    String trimmedValue = value.trim();
    if(Strings.isNullOrEmpty(trimmedValue) || (spssVariable instanceof SPSSStringVariable)) return value;

    switch(SpssVariableTypeMapper.getSpssNumericDataType(spssVariable)) {
      case ADATE:
        return new SpssDateValueConverters.ADateValueConverter().convert(trimmedValue);
      case DATE:
        return new SpssDateValueConverters.DateValueConverter().convert(trimmedValue);
      case DATETIME:
        return new SpssDateValueConverters.DateTimeValueConverter().convert(trimmedValue);
      default:
        return value;
    }
  }

}
