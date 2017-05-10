package org.gradoop.flink.algorithms.fsm.cross_level.vector_mining;


public class SearchTreeNode {
  private final int[][] pattern;
  private final int[] occurrences;

  public SearchTreeNode(int[][] pattern, int[] occurrences) {
    this.pattern = pattern;
    this.occurrences = occurrences;
  }

  public int[] getOccurrences() {
    return occurrences;
  }

  public int[][] getPattern() {
    return pattern;
  }

  public int getFrequency() {
    return occurrences.length;
  }
}
