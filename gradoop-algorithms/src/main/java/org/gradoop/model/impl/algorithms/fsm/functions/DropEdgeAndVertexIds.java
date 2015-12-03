package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.model.impl.algorithms.fsm.tuples.FSMEdge;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 03.12.15.
 */
public class DropEdgeAndVertexIds implements MapFunction<FSMEdge, Tuple4<GradoopId, String, String, String>> {
  @Override
  public Tuple4<GradoopId, String, String, String>
  map(FSMEdge edge) throws Exception {
    return new Tuple4<>(edge.getGraphId(), edge.getSourceLabel(), edge
      .getEdgeLabel(), edge.getTargetLabel());
  }
}
