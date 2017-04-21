package org.gradoop.flink.algorithms.fsm.xmd.xvm;

public interface CrossLevelFrequentVectors {

  int[][][] mine(int[][][] vectors, float minSupport );
}
