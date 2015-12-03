package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.algorithms.fsm.tuples.FSMEdge;
import org.gradoop.model.impl.id.GradoopId;


public class EdgeWithVertexLabelsAsTuple
  implements JoinFunction<
  Tuple6<GradoopId, GradoopId, String, GradoopId, String,  GradoopId>,
  Tuple2<GradoopId, String>, FSMEdge> {

  @Override
  public FSMEdge  join(
    Tuple6<GradoopId, GradoopId, String, GradoopId, String, GradoopId> tuple6,
    Tuple2<GradoopId, String> target) throws Exception {

    return new FSMEdge(
      tuple6.f0,
      tuple6.f1,
      tuple6.f2,
      tuple6.f3,
      tuple6.f4,
      tuple6.f5,
      target.f1
    );
  }
}
