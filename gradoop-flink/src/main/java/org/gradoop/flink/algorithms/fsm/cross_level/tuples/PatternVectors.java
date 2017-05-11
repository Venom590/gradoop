package org.gradoop.flink.algorithms.fsm.cross_level.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by peet on 11.05.17.
 */
public class PatternVectors extends Tuple2<int[], int[][][]> {

  public PatternVectors() {
  }

  public PatternVectors(int[] pattern, int[][][] vectors) {
    super(pattern, vectors);
  }

  public int[] getPattern() {
    return f0;
  }

  public int[][][] getVectors() {
    return f1;
  }

  public void setVectors(int[][][] vectors) {
    this.f1 = vectors;
  }
}
