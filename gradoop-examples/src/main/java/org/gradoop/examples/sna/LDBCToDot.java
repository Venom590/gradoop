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
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.EdgeCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;

import java.util.Arrays;

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
    String inputDir  = "/home/stephan/programs/ldbc_snb_datagen/social_network/";//args[0];
    String outputDir = "/home/stephan/programs/";//args[1];

    LogicalGraph epgmDatabase = readLogicalGraph(inputDir);


    writeDotGraph(epgmDatabase, outputDir);
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
