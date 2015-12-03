package org.gradoop.model.impl.algorithms.fsm.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.tuples.FSMEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.GSpanEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.GSpanGraph;
import org.gradoop.model.impl.id.GradoopId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GSpanGraphBuilder implements
  GroupReduceFunction<FSMEdge, GSpanGraph> {

  Short currentVertexId = 0;
  Short currentEdgeId = 0;
  Map<GradoopId, Short> vertexMap = new HashMap<>();

  @Override
  public void reduce(Iterable<FSMEdge> fsmEdges,
    Collector<GSpanGraph> collector) throws Exception {

    List<GSpanEdge> edges = new ArrayList<>();

    for(FSMEdge fsmEdge : fsmEdges) {
      Short newSourceId = getNewVertexId(fsmEdge.getSourceId());
      Short newTargetId = getNewVertexId(fsmEdge.getTargetId());

      edges.add(new GSpanEdge(
        newSourceId,
        fsmEdge.getSourceLabel(),
        currentEdgeId++,
        fsmEdge.getEdgeLabel(),
        newTargetId,
        fsmEdge.getTargetLabel()
      ));
    }

    collector.collect(GSpanGraph.fromEdges(edges));
  }

  protected Short getNewVertexId(GradoopId oldSourceId) {
    Short newSourceId = vertexMap.get(oldSourceId);

    if(newSourceId == null) {
      newSourceId = currentVertexId++;
      vertexMap.put(oldSourceId, newSourceId);
    }

    return newSourceId;
  }
}
