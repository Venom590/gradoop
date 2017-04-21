package org.gradoop.flink.algorithms.fsm.cross_level.xfi;

public interface CrossLevelFrequentVectors {

  int[][][] mine(int[][][] transactions, float minSupport );
}
