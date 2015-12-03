package org.gradoop.model.impl.algorithms.fsm.tuples;

import org.apache.flink.api.java.tuple.Tuple6;
import scala.util.parsing.combinator.testing.Str;

/**
 * (fromTime, fromLabel, inDirection, viaLabel, toTime, toLabel )
 */
public class DfsStep
  extends Tuple6<Short, String, Boolean, String, Short, String>
  implements Comparable<DfsStep>{


  public DfsStep() {
  }

  @Override
  public int compareTo(DfsStep o) {
    return 0;
  }

  public void setInDirection(boolean inDirection) {
    this.f2 = inDirection;
  }

  public void setEdgeLabel(String edgeLabel) {
    this.f3 = edgeLabel;
  }

  public void setFromTime(short fromTime) {
    this.f0 = fromTime;
  }

  public void setToTime(short toTime) {
    this.f4 = toTime;
  }

  public void setFromLabel(String fromLabel) {
    this.f1 = fromLabel;
  }

  public void setToLabel(String toLabel) {
    this.f5 = toLabel;
  }

  public static DfsStep fromEdge(GSpanEdge edge, boolean inDirection) {
    DfsStep dfsStep = new DfsStep();

    dfsStep.setFromTime((short) 0);
    dfsStep.setInDirection(inDirection);
    dfsStep.setEdgeLabel(edge.getEdgeLabel());
    dfsStep.setToTime((short) (edge.isLoop() ? 0 : 1));

    if (inDirection) {
      dfsStep.setFromLabel(edge.getSourceLabel());
      dfsStep.setToLabel(edge.getTargetLabel());
    } else {
      dfsStep.setFromLabel(edge.getTargetLabel());
      dfsStep.setToLabel(edge.getSourceLabel());
    }
    return dfsStep;
  }

  public boolean isInDirection() {
    return this.f2;
  }

  public String getFromLabel() {
    return this.f1;
  }

  public Short getFromId() {
    return this.f0;
  }

  public Short getToId() {
    return this.f4;
  }

  public String getToLabel() {
    return this.f5;
  }

  public short getFromTime() {
    return this.f0;
  }

  public short getToTime() {
    return this.f4;
  }

  public boolean isBackward() {
    return getFromTime() > getToTime();
  }

  public boolean isLoop() {
    return getFromTime() == getToTime();
  }

  public boolean isForward() {
    return !isBackward();
  }

  public String getEdgeLabel() {
    return this.f3;
  }
}
