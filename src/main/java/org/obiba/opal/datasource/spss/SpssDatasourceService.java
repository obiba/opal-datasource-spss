package org.obiba.opal.datasource.spss;

import org.json.JSONObject;
import org.obiba.magma.DatasourceFactory;
import org.obiba.magma.datasource.spss.support.SpssDatasourceFactory;
import org.obiba.opal.spi.datasource.AbstractDatasourceService;
import org.obiba.opal.spi.datasource.DatasourceUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpssDatasourceService extends AbstractDatasourceService {

  private static final Logger log = LoggerFactory.getLogger(SpssDatasourceService.class);

  @Override
  public String getName() {
    return "opal-datasource-spss";
  }

  @Override
  public DatasourceFactory createDatasourceFactory(DatasourceUsage usage, JSONObject parameters) {
    SpssDatasourceFactory spssDatasourceFactory = new SpssDatasourceFactory();
    spssDatasourceFactory.setName(getName());
    spssDatasourceFactory.setFile(resolvePath(parameters.optString("file")));
    spssDatasourceFactory.setEntityType(parameters.optString("entity_type"));
    spssDatasourceFactory.setCharacterSet(parameters.optString("charset"));
    spssDatasourceFactory.setLocale(parameters.optString("locale"));

    return spssDatasourceFactory;
  }

}