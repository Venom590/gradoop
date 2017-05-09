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

package org.gradoop.flink.algorithms.fsm.xmd.functions.preprocessing;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.xmd.comparison.DFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.xmd.comparison.DirectedDFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.xmd.comparison.UndirectedDFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.xmd.config.DIMSpanConstants;
import org.gradoop.flink.algorithms.fsm.xmd.config.XMDConfig;
import org.gradoop.flink.algorithms.fsm.xmd.model.GraphUtils;
import org.gradoop.flink.algorithms.fsm.xmd.model.SearchGraphUtils;
import org.gradoop.flink.algorithms.fsm.xmd.model.UnsortedSearchGraphUtils;
import org.gradoop.flink.algorithms.fsm.xmd.tuples.EncodedMDGraph;
import org.gradoop.flink.algorithms.fsm.xmd.tuples.MultidimensionalGraph;

import java.util.Arrays;
import java.util.Map;

/**
 * Encodes edge labels to integers.
 * Drops edges with infrequent labels and isolated vertices.
 */
public class Encode extends RichMapFunction<MultidimensionalGraph, EncodedMDGraph> {

  /**
   * label dictionary
   */
  private Map<String, Integer> dictionary = Maps.newHashMap();

  /**
   * flag to enable graph sorting (true=enabled)
   */
  private final boolean sortGraph;

  /**
   * comparator used for graph sorting
   */
  private final DFSBranchComparator branchComparator;

  /**
   * util methods to interpret and manipulate int-array encoded graphs
   */
  private final SearchGraphUtils graphUtils = new UnsortedSearchGraphUtils();

  /**
   * Constructor
   *
   * @param fsmConfig FSM configuration
   */
  public Encode(XMDConfig fsmConfig) {
    sortGraph = fsmConfig.isBranchConstraintEnabled();
    branchComparator = fsmConfig.isDirected() ?
      new DirectedDFSBranchComparator() :
      new UndirectedDFSBranchComparator();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    // create inverse dictionary at broadcast reception
    String[] broadcast = getRuntimeContext()
      .<String[]>getBroadcastVariable(DIMSpanConstants.LABEL_DICTIONARY).get(0);

    for (int i = 0; i < broadcast.length; i++) {
      dictionary.put(broadcast[i], i);
    }
  }

  @Override
  public EncodedMDGraph map(MultidimensionalGraph inGraph) throws Exception {

    // VERTICES

    String[][][] vertexData = inGraph.getVertexData();

    int[] vertexIdMap = new int[vertexData.length];

    int[] vertexLabels = new int[0];

    int[][] dimensions = new int[0][];

    int oldId = 0;
    int newId = 0;
    for (String[][] data : vertexData) {

      String stringLabel = data[0][0];
      Integer intLabel = dictionary.get(stringLabel);

      if (intLabel != null) {
        // vertex has frequent label

        for (int i = 1; i < data.length; i++) {
          String[] stringDimension = data[i];
          int[] intDimension = new int[] {newId};

          for (String stringValue : stringDimension) {
            Integer intValue = dictionary.get(stringValue);

            if (intValue != null) {
              // dimension (0) or value (>0) is frequent

              ArrayUtils.add(intDimension, intValue);
            } else {
              // children of infrequent values cannot be frequent anymore
              break;
            }
          }

          if (intDimension.length > 1) {
            ArrayUtils.add(dimensions, intDimension);
          }
        }
        vertexIdMap[oldId] = newId;
        ArrayUtils.add(vertexLabels, intLabel);

        newId++;
      }
      oldId++;
    }

    Arrays.sort(dimensions);

    // EDGES

    int[][] dfsCodes = new int[0][];

    String[] edgeLabels = inGraph.getEdgeLabels();

    for (int edgeId = 0; edgeId < edgeLabels.length; edgeId++) {
      Integer edgeLabel = dictionary.get(edgeLabels[edgeId]);

      if (edgeLabel != null) {
        // edge has frequent label

        int[] edge = inGraph.getEdges()[edgeId];
        int sourceId = vertexIdMap[edge[0]];
        int sourceLabel = vertexLabels[sourceId];

        if (sourceId >= 0) {
          // source vertex has frequent label

          int targetId = vertexIdMap[edge[1]];
          int targetLabel = vertexLabels[targetId];

          if (targetId >= 0) {
            int[] dfsCode = sourceLabel <= targetLabel ?
              graphUtils.multiplex(
                sourceId, sourceLabel, true, edgeLabel, targetId, targetLabel) :
              graphUtils.multiplex(
                targetId, targetLabel, false, edgeLabel, sourceId, sourceLabel);

            dfsCodes = ArrayUtils.add(dfsCodes, dfsCode);
          }
        }
      }
    }

    // optionally sort 1-edge DFS codes
    if (sortGraph) {
      Arrays.sort(dfsCodes, branchComparator);
    }

    // multiplex 1-edge DFS codes
    int[] outGraph = new int[dfsCodes.length * GraphUtils.EDGE_LENGTH];

    int i = 0;
    for (int[] dfsCode : dfsCodes) {
      System.arraycopy(dfsCode, 0, outGraph, i * 6, GraphUtils.EDGE_LENGTH);
      i++;
    }

    return new EncodedMDGraph(outGraph, dimensions);
  }
}
