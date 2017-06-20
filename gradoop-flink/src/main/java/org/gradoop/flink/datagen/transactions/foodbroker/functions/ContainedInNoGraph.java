package org.gradoop.flink.datagen.transactions.foodbroker.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.EPGMGraphElement;

/**
 * Created by peet on 20.06.17.
 */
public class ContainedInNoGraph<EL extends EPGMGraphElement> implements FilterFunction<EL> {

  @Override
  public boolean filter(EL value) throws Exception {
    return value.getGraphIds() == null || value.getGraphIds().isEmpty();
  }
}
