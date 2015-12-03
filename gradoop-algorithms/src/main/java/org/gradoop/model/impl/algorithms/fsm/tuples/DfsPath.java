package org.gradoop.model.impl.algorithms.fsm.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (vertexIdDiscoveryTimes,edgeIdDiscoveryTimes)
 */
public class DfsPath extends Tuple2<Short[], Short[]> {

  public DfsPath() {

  }

  public static DfsPath fromEdge(GSpanEdge edge, boolean inDirection) {
    DfsPath embedding = new DfsPath();

    Short[] vertexTimes = null;

    if (edge.isLoop()) {
      vertexTimes = new Short[] {edge.getSourceId()};
    } else if (inDirection) {
      vertexTimes = new Short[] {edge.getSourceId(), edge.getTargetId()};
    } else {
      vertexTimes = new Short[] {edge.getTargetId(), edge.getSourceId()};
    }

    embedding.setVertexTimes(vertexTimes);
    embedding.setEdgeTimes(new Short[] {edge.getEdgeId()});

    return embedding;
  }

  public void setVertexTimes(Short[] vertexTimes) {
    this.f0 = vertexTimes;
  }

  public void setEdgeTimes(Short[] edgeTimes) {
    this.f1 = edgeTimes;
  }
}
