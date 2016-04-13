package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.impl.algorithms.fsm.tuples.StringLabeledEdge;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Created by peet on 13.04.16.
 */
public class StringEdgeLabel implements
  KeySelector<Tuple2<GradoopId, StringLabeledEdge>, String> {
  @Override
  public String getKey(Tuple2<GradoopId, StringLabeledEdge> pair) throws
    Exception {
    return pair.f1.getLabel();
  }
}
