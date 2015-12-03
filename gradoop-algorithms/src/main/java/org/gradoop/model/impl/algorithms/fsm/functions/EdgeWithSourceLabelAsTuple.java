package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;


public class EdgeWithSourceLabelAsTuple<E extends EPGMEdge>
  implements FlatJoinFunction<E, Tuple2<GradoopId, String>,
  Tuple6<GradoopId, GradoopId, String,GradoopId, String, GradoopId>> {

  @Override
  public void join(E edge, Tuple2<GradoopId, String> source, Collector<
      Tuple6<GradoopId, GradoopId, String, GradoopId, String, GradoopId>>
    collector) throws Exception {

    for(GradoopId graphId : edge.getGraphIds()) {
      collector.collect(new Tuple6<>(
        graphId,
        source.f0,
        source.f1,
        edge.getId(),
        edge.getLabel(),
        edge.getTargetId()
      ));
    }
  }
}
