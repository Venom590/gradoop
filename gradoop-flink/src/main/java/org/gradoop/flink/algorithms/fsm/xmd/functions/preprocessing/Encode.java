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
import org.gradoop.flink.algorithms.fsm.common.config.FSMConstants;
import org.gradoop.flink.algorithms.fsm.common.comparison.DFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.common.comparison.DirectedDFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.common.comparison.UndirectedDFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.xmd.config.XMDConfig;
import org.gradoop.flink.algorithms.fsm.xmd.model.GraphUtils;
import org.gradoop.flink.algorithms.fsm.xmd.model.SearchGraphUtils;
import org.gradoop.flink.algorithms.fsm.xmd.model.UnsortedSearchGraphUtils;
import org.gradoop.flink.algorithms.fsm.xmd.tuples.EncodedMultilevelGraph;
import org.gradoop.flink.algorithms.fsm.xmd.tuples.MultilevelGraph;

import java.util.Arrays;
import java.util.Map;

/**
 * Encodes edge labels to integers.
 * Drops edges with infrequent labels and isolated vertices.
 */
public class Encode extends RichMapFunction<MultilevelGraph, EncodedMultilevelGraph> {

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
      .<String[]>getBroadcastVariable(FSMConstants.LABEL_DICTIONARY).get(0);

    for (int i = 0; i < broadcast.length; i++) {
      dictionary.put(broadcast[i], i);
    }
  }

  @Override
  public EncodedMultilevelGraph map(MultilevelGraph inGraph) throws Exception {

    // VERTICES

    String[][] stringVertexLabels = inGraph.getVertexLabels();

    int[] vertexIdMap = new int[stringVertexLabels.length];
    int[] vertexTopLevels = new int[0];
    int[][] vertexLowerLevels = new int[0][];

    int oldId = 0;
    int newId = 0;
    for (String[] stringVertexLabel : stringVertexLabels) {

      String stringTopLevel = stringVertexLabel[0];
      Integer intTopLevel = dictionary.get(stringTopLevel);

      if (intTopLevel != null) {
        // vertex has frequent top level

        int depth = stringVertexLabel.length;
        int[] intLevels = new int[0];

        for (int i = 1; i < depth; i++) {
          String stringLevel = stringVertexLabel[i];
          Integer intLevel = dictionary.get(stringLevel);

          if (intLevel != null) {
            ArrayUtils.add(intLevels, intLevel);
          } else {
            break;
          }
        }

        vertexIdMap[oldId] = newId;
        ArrayUtils.add(vertexTopLevels, intTopLevel);
        ArrayUtils.add(vertexLowerLevels, intLevels);

        newId++;
      }
      oldId++;
    }

    Arrays.sort(vertexLowerLevels);

    // EDGES

    int[][] dfsCodes = new int[0][];

    String[] edgeLabels = inGraph.getEdgeLabels();

    for (int edgeId = 0; edgeId < edgeLabels.length; edgeId++) {
      Integer edgeLabel = dictionary.get(edgeLabels[edgeId]);

      if (edgeLabel != null) {
        // edge has frequent label

        int[] edge = inGraph.getEdges()[edgeId];
        int sourceId = vertexIdMap[edge[0]];
        int sourceLabel = vertexTopLevels[sourceId];

        if (sourceId >= 0) {
          // source vertex has frequent label

          int targetId = vertexIdMap[edge[1]];
          int targetLabel = vertexTopLevels[targetId];

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

    return new EncodedMultilevelGraph(outGraph, vertexLowerLevels);
  }
}
