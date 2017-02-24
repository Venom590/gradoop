/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.examples.sna;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.s1ck.ldbc.LDBCToFlink;
import org.s1ck.ldbc.tuples.LDBCEdge;
import org.s1ck.ldbc.tuples.LDBCVertex;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The benchmark program executes the following workflow:
 *
 * 1) Extract subgraph with:
 *    - vertex predicate: must be of type 'Person'
 *    - edge predicate: must be of type 'knows'
 * 2) Group the subgraph using the vertex attributes 'city' and 'gender' and
 *    - count the number of vertices represented by each super vertex
 *    - count the number of edges represented by each super edge
 * 3) Aggregate the grouped graph:
 *    - add the total vertex count as new graph property
 *    - add the total edge count as new graph property
 */
public class LDBCToDot extends AbstractRunner implements
  ProgramDescription {

  /**
   * Runs the example program.
   *
   * Need a (possibly HDFS) input directory that contains
   *  - nodes.json
   *  - edges.json
   *  - graphs.json
   *
   * Needs a (possibly HDFS) output directory to write the resulting graph to.
   *
   * @param args args[0] = input dir, args[1] output dir
   * @throws Exception
   */
  @SuppressWarnings({
    "unchecked",
    "Duplicates"
  })
  public static void main(String[] args) throws Exception {
//    Preconditions.checkArgument(
//      args.length == 2, "input dir and output dir required");
//    String inputDir  = "/home/stephan/programs/ldbc_snb_datagen/social_network/";//args[0];
//    String outputDir = "/home/stephan/programs/";//args[1];
    String inputDir = "C:\\Users\\Stephan\\Desktop\\ldbc_snb_datagen\\social_network";
    String outputDir = "C:\\Users\\Stephan\\Desktop\\ldbcout";
    File file = new File(inputDir);
    if (file.exists()) {
      System.out.println("file.getPath() = " + file.getPath());
    }
    System.out.println("file = " + file.isDirectory());

    LDBCToFlink ldbcToFlink = new LDBCToFlink(file.getPath(),
      ExecutionEnvironment.getExecutionEnvironment());

    DataSet<LDBCVertex> vertices = ldbcToFlink.getVertices();
    DataSet<LDBCEdge> edges = ldbcToFlink.getEdges();

    DataSet<Vertex> epgmVertex = vertices
      .map(new MapFunction<LDBCVertex, Vertex>() {
        @Override
        public Vertex map(LDBCVertex ldbcVertex) throws Exception {
          Vertex vertex = new VertexFactory().createVertex(ldbcVertex.getLabel());
          vertex.setProperty("id", ldbcVertex.getVertexId());
          for (Map.Entry<String, Object> entry : ldbcVertex.getProperties().entrySet()) {
            vertex.setProperty(entry.getKey(), entry.getValue());
          }
          return vertex;
        }
      });
    DataSet<Tuple2<String, GradoopId>> ids = epgmVertex
      .map(new MapFunction<Vertex, Tuple2<String, GradoopId>>() {
        @Override
        public Tuple2<String, GradoopId> map(Vertex vertex) throws Exception {
          return new Tuple2<String, GradoopId>(vertex.getPropertyValue("id").getString(), vertex.getId());
        }
      });

    DataSet<Edge> epgmEdge = edges
      .map(new RichMapFunction<LDBCEdge, Edge>() {
        @Override
        public void open(Configuration parameters) throws Exception {
          super.open(parameters);
          List<Vertex> vertices = getRuntimeContext().getBroadcastVariable("vertices");
        }

        @Override
        public Edge map(LDBCEdge ldbcEdge) throws Exception {
          Edge edge = new EdgeFactory().createEdge(ldbcEdge.getLabel(), GradoopId.NULL_VALUE,
            GradoopId.NULL_VALUE);
          for (Map.Entry<String, Object> entry : ldbcEdge.getProperties().entrySet()) {
            edge.setProperty(entry.getKey(), entry.getValue());
          }
          return edge;
        }
      });

    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(getExecutionEnvironment());
    LogicalGraph lg = LogicalGraph.fromDataSets(epgmVertex,epgmEdge,config);

    lg.writeTo(new JSONDataSink(outputDir+"\\graph1.json", outputDir+"\\vertex1.json",
      outputDir+"\\edge1.json",config));
    getExecutionEnvironment().setParallelism(1);
getExecutionEnvironment().execute();
//    vertices.print();

//    System.out.println("vertices = " + vertices.collect().size());
//    for (LDBCVertex ldbcVertex : vertices.collect()) {
//      System.out.println("ldbcVertex = " + ldbcVertex);
//    }

//    LogicalGraph epgmDatabase = readLogicalGraph(inputDir);
//
//
//    writeDotGraph(epgmDatabase, outputDir);
  }

  protected static void writeDotGraph(LogicalGraph graph, String directory)
    throws Exception {
    directory = appendSeparator(directory);
    graph.writeTo(new DOTDataSink(directory ,false));

    getExecutionEnvironment().execute();
  }

  private static String appendSeparator(final String directory) {
    final String fileSeparator = System.getProperty("file.separator");
    String result = directory;
    if (!directory.endsWith(fileSeparator)) {
      result = directory + fileSeparator;
    }
    return result;
  }


  @Override
  public String getDescription() {
    return LDBCToDot.class.getName();
  }
}
