package org.gradoop.model.impl.algorithms.fsm.tuples;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Collections;
import java.util.List;


public class GSpanGraph
  extends Tuple3<GradoopId, GSpanEdge[], DfsCode[]> {



  public GSpanGraph() {

  }

  private void add(DfsCode dfsCode) {
    if (getDfsCodes() == null) {
      setDfsCodes(new DfsCode[] {dfsCode});
    } else {
      ArrayUtils.add(getDfsCodes(), dfsCode);
    }
  }

  public DfsCode[] getDfsCodes() {
    return this.f2;
  }

  public void setDfsCodes(DfsCode[] dfsCodes) {
    this.f2 = dfsCodes;
  }

  public static GSpanGraph fromEdges(List<GSpanEdge> edges) {
    GSpanGraph graph = new GSpanGraph();

    Collections.sort(edges);

    graph.setId(GradoopId.get());

    graph.setEdges(edges.toArray(new GSpanEdge[edges.size()]));

    boolean inDirection = true;
    GSpanEdge lastEdge = null;
    DfsCode dfsCode = null;

    for (GSpanEdge edge : edges) {

      if (lastEdge == null || edge.compareTo(lastEdge) > 0) {
        // create new DFS code
        inDirection =
          edge.getSourceLabel().compareTo(edge.getTargetLabel()) <= 0;

        dfsCode = DfsCode.fromStep(DfsStep.fromEdge(edge, inDirection));
        graph.add(dfsCode);
      }

      dfsCode.add(DfsPath.fromEdge(edge, inDirection));
      lastEdge = edge;
    }

    return graph;
  }

  public void setId(GradoopId id) {
    this.f0 = id;
  }

  public void setEdges(GSpanEdge[] edges) {
    this.f1 = edges;
  }

}
