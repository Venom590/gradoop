package org.gradoop.model.impl.algorithms.fsm.tuples;


import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (dfsCode,dfsEmbeddings)
 */
public class DfsCode extends Tuple2<DfsStep[], DfsPath[]> {

  public DfsCode() {

  }

  public static DfsCode fromStep(DfsStep step) {
    DfsCode dfsCode = new DfsCode();

    DfsStep[] steps = new DfsStep[1];
    steps[0] = step;

    dfsCode.setSteps(steps);

    return dfsCode;
  }

  public void setSteps(DfsStep[] steps) {
    this.f0 = steps;
  }

  public void add(DfsPath dfsPath) {
    if(getPaths() == null) {
      setPaths(new DfsPath[]{dfsPath});
    }
    ArrayUtils.add(getPaths(), dfsPath);
  }

  public void setPaths(DfsPath[] embeddings) {
    this.f1 = embeddings;
  }

  private DfsPath[] getPaths() {
    return this.f1;
  }

  public DfsStep[] getSteps() {
    return this.f0;
  }

  public int getVertexCount() {
    DfsStep lastStep = getLastStep();

    return lastStep.isBackward() ?
      lastStep.getFromTime() : lastStep.getToTime() ;
  }

  private DfsStep getLastStep() {
    return getSteps()[getSteps().length - 1];
  }

  public int getEdgeCount() {
    return getSteps().length;
  }
}
