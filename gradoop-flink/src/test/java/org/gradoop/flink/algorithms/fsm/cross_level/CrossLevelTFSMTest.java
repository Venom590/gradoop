package org.gradoop.flink.algorithms.fsm.cross_level;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

/**
 * Created by peet on 10.05.17.
 */
public class CrossLevelTFSMTest extends GradoopFlinkTestBase {
  @Test
  public void execute() throws Exception {

    String graphs =
      "g1[(v1:A {dim_a:\"a1.a11\"})-[:b]->(:B)<-[:b]-(:A dim_a:\"a1.a11\")-[:a]->(v1)]" +
      "g2[(v1:A)-[:b]->(:B)<-[:b]->(:A)-[:a]->(v1)]" +
      "g3[(v1:A)-[:b]->(:B)<-[:b]->(:A)-[:a]->(v1)]";

  }

}