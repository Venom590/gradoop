package org.gradoop.flink.algorithms.fsm.cross_level;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class CrossLevelTFSMIntegrationTest extends GradoopFlinkTestBase {

  @Test
  public void testExecute() throws Exception {

    String asciiGraphs =
      "g1[(v1:A {_dl_0:\"1\",_dl_1:\"1\"})-[:a]->(:B{_dl_0:\"1\",_dl_1:\"1\",_dl_2:\"1\"})-[:a]->(:C)-[:a]->(v1)]" +
      "g2[(v2:A {_dl_0:\"1\",_dl_1:\"1\"})-[:a]->(:B{_dl_0:\"1\",_dl_1:\"1\",_dl_2:\"2\"})-[:a]->(:C)-[:a]->(v2)]" +
      "g3[(v3:A {_dl_0:\"1\",_dl_1:\"2\"})-[:a]->(:B{_dl_0:\"1\",_dl_1:\"1\",_dl_2:\"2\"})-[:a]->(:C)-[:a]->(v3)]" +

      // full graph
      "f1[(v4:A)-[:a]->(:B)-[:a]->(:C)-[:a]->(v4)]" +
      "f2[(v5:A)-[:a]->(:B{_dl_0:\"1\"})-[:a]->(:C)-[:a]->(v5)]" +
      "f3[(v6:A)-[:a]->(:B{_dl_0:\"1\",_dl_1:\"1\"})-[:a]->(:C)-[:a]->(v6)]" +
      "f4[(v7:A {_dl_0:\"1\"})-[:a]->(:B)-[:a]->(:C)-[:a]->(v7)]" +
      "f5[(v8:A {_dl_0:\"1\"})-[:a]->(:B{_dl_0:\"1\"})-[:a]->(:C)-[:a]->(v8)]" +
      "f6[(v9:A {_dl_0:\"1\"})-[:a]->(:B{_dl_0:\"1\",_dl_1:\"1\"})-[:a]->(:C)-[:a]->(v9)]" +

      // without C-A edge
      "a1[(:A)-[:a]->(:B)-[:a]->(:C)]" +
      "a2[(:A)-[:a]->(:B{_dl_0:\"1\"})-[:a]->(:C)]" +
      "a3[(:A)-[:a]->(:B{_dl_0:\"1\",_dl_1:\"1\"})-[:a]->(:C)]" +
      "a4[(:A {_dl_0:\"1\"})-[:a]->(:B)-[:a]->(:C)]" +
      "a5[(:A {_dl_0:\"1\"})-[:a]->(:B{_dl_0:\"1\"})-[:a]->(:C)]" +
      "a6[(:A {_dl_0:\"1\"})-[:a]->(:B{_dl_0:\"1\",_dl_1:\"1\"})-[:a]->(:C)]" +

      // without A-B edge
      "b1[(:B)-[:a]->(:C)-[:a]->(:A)]" +
      "b2[(:B{_dl_0:\"1\"})-[:a]->(:C)-[:a]->(:A)]" +
      "b3[(:B{_dl_0:\"1\",_dl_1:\"1\"})-[:a]->(:C)-[:a]->(:A)]" +
      "b4[(:B)-[:a]->(:C)-[:a]->(:A {_dl_0:\"1\"})]" +
      "b5[(:B{_dl_0:\"1\"})-[:a]->(:C)-[:a]->(:A {_dl_0:\"1\"})]" +
      "b6[(:B{_dl_0:\"1\",_dl_1:\"1\"})-[:a]->(:C)-[:a]->(:A {_dl_0:\"1\"})]" +

      // without B-C edge
      "c1[(:C)-[:a]->(:A)-[:a]->(:B)]" +
      "c2[(:C)-[:a]->(:A)-[:a]->(:B{_dl_0:\"1\"})]" +
      "c3[(:C)-[:a]->(:A)-[:a]->(:B{_dl_0:\"1\",_dl_1:\"1\"})]" +
      "c4[(:C)-[:a]->(:A {_dl_0:\"1\"})-[:a]->(:B)]" +
      "c5[(:C)-[:a]->(:A {_dl_0:\"1\"})-[:a]->(:B{_dl_0:\"1\"})]" +
      "c6[(:C)-[:a]->(:A {_dl_0:\"1\"})-[:a]->(:B{_dl_0:\"1\",_dl_1:\"1\"})]" +

      // only A-B edge
      "d1[(:A)-[:a]->(:B)]" +
      "d2[(:A)-[:a]->(:B{_dl_0:\"1\"})]" +
      "d3[(:A)-[:a]->(:B{_dl_0:\"1\",_dl_1:\"1\"})]" +
      "d4[(:A {_dl_0:\"1\"})-[:a]->(:B)]" +
      "d5[(:A {_dl_0:\"1\"})-[:a]->(:B{_dl_0:\"1\"})]" +
      "d6[(:A {_dl_0:\"1\"})-[:a]->(:B{_dl_0:\"1\",_dl_1:\"1\"})]" +

//      // only B-C edge
      "e1[(:B)-[:a]->(:C)]" +
      "e2[(:B{_dl_0:\"1\"})-[:a]->(:C)]" +
      "e3[(:B{_dl_0:\"1\",_dl_1:\"1\"})-[:a]->(:C)]" +

      // only C-A edge
      "c1[(:C)-[:a]->(:A)]" +
      "c4[(:C)-[:a]->(:A {_dl_0:\"1\"})]" +

      "";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiGraphs);

    System.out.println(loader.getVertices());

  }
}