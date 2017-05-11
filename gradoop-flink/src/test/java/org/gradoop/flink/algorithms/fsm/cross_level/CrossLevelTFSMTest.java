package org.gradoop.flink.algorithms.fsm.cross_level;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

/**
 * Created by peet on 10.05.17.
 */
public class CrossLevelTFSMTest extends GradoopFlinkTestBase {
  @Test
  public void execute() throws Exception {

    String asciiGraphs =
      "g1[(v1:A {_dl_1:\"fuck\"})-[:b]->(:B)<-[:b]-(:A)-[:a]->(v1)]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiGraphs);

    System.out.println(loader.getVertices());

  }
}