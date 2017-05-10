package org.gradoop.flink.algorithms.fsm.cross_level.vector_mining;


import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Comparator;

public class CrossLevelVectorWithCountComparator implements Comparator<WithCount<int[][]>> {

  private final CrossLevelVectorComparator vectorComparator = new CrossLevelVectorComparator();


  @Override
  public int compare(WithCount<int[][]> a, WithCount<int[][]> b) {
    return vectorComparator.compare(a.getObject(), b.getObject());
  }
}
