package org.gradoop.model.impl.algorithms.fsm.tuples;

import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.model.impl.id.GradoopId;


public class GSpanEdge
  extends Tuple6<Short, String, Short, String, Short, String>
  implements Comparable<GSpanEdge> {

  public GSpanEdge(){

  }

  public GSpanEdge(Short sourceId, String sourceLabel, Short edgeId,
    String edgeLabel, Short targetId, String targetLabel) {

    this.f0 = sourceId;
    this.f1 = sourceLabel;
    this.f2 = edgeId;
    this.f3 = edgeLabel;
    this.f4 = targetId;
    this.f5 = targetLabel;
  }

  @Override
  public int compareTo(GSpanEdge other) {
    int comparison = this.getSourceLabel().compareTo(other.getSourceLabel());

    if(comparison == 0) {
      comparison = this.getEdgeLabel().compareTo(other.getEdgeLabel());

      if(comparison == 0) {
        comparison = this.getTargetLabel().compareTo(other.getTargetLabel());
      }
    }

    return comparison;
  }

  public String getSourceLabel() {
    return this.f1;
  }

  public String getEdgeLabel() {
    return this.f3;
  }

  public String getTargetLabel() {
    return this.f5;
  }

  public Short getSourceId() {
    return this.f0;
  }

  public Short getTargetId() {
    return this.f4;
  }

  public boolean isLoop() {
    return getSourceId().equals(getTargetId());
  }

  public Short getEdgeId() {
    return this.f2;
  }

}
