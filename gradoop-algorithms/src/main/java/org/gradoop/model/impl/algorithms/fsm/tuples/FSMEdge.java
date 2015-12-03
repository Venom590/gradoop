package org.gradoop.model.impl.algorithms.fsm.tuples;


import org.apache.flink.api.java.tuple.Tuple7;
import org.gradoop.model.impl.id.GradoopId;

public class FSMEdge extends
  Tuple7<GradoopId, GradoopId, String, GradoopId, String, GradoopId, String>
  implements Comparable<FSMEdge> {

  public FSMEdge() {

  }

  public FSMEdge(GradoopId graphId,
    GradoopId sourceId, String sourceLabel,
    GradoopId edgeId, String edgeLabel,
    GradoopId targetId, String targetLabel) {

    super(graphId, sourceId, sourceLabel,
      edgeId, edgeLabel, targetId, targetLabel);
  }


  public GradoopId getGraphId() {
    return this.f0;
  }

  public void setGraphId(GradoopId id) {
    this.f0 = id;
  }

  public GradoopId getSourceId() {
    return this.f1;
  }

  public void setSourceId(GradoopId id) {
    this.f1 = id;
  }

  public String getSourceLabel() {
    return this.f2;
  }

  public void setSourceLabel(String label) {
    this.f2 = label;
  }

  public GradoopId getEdgeId() {
    return this.f3;
  }

  public void setEdgeId(GradoopId id) {
    this.f3 = id;
  }

  public String getEdgeLabel() {
    return this.f4;
  }

  public void setEdgeLabel(String label) {
    this.f4 = label;
  }

  public GradoopId getTargetId() {
    return this.f5;
  }

  public void setTargetId(GradoopId id) {
    this.f5 = id;
  }

  public String getTargetLabel() {
    return this.f6;
  }

  public void setTargetLabel(String label) {
    this.f6 = label;
  }



  @Override
  public int compareTo(FSMEdge other) {
    int comparison = this.getSourceLabel().compareTo(other.getSourceLabel());

    if(comparison == 0) {
      comparison = this.getEdgeLabel().compareTo(other.getEdgeLabel());

      if(comparison == 0) {
        comparison = this.getTargetLabel().compareTo(other.getTargetLabel());
      }
    }

    return comparison;
  }
}
