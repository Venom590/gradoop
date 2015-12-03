package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.id.GradoopId;

public class VertexLabelMapper<V extends EPGMVertex> implements
  FlatMapFunction<V, Tuple3<GradoopId, String, Boolean>> {
  @Override
  public void flatMap(V vertex,
    Collector<Tuple3<GradoopId, String, Boolean>> collector) throws Exception {

    String vertexLabel = vertex.getLabel();

    collector.collect(new Tuple3<>(vertex.getId(), vertexLabel, true));

    for(GradoopId graphId : vertex.getGraphIds()) {
      collector.collect(new Tuple3<>(graphId, vertexLabel, false));
    }
  }
}
