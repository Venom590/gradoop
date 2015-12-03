package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.id.GradoopId;

public class ContainsVertexId
  implements FilterFunction<Tuple3<GradoopId, String, Boolean>> {

  @Override
  public boolean filter(
    Tuple3<GradoopId, String, Boolean> triple) throws Exception {
    return triple.f2;
  }
}
