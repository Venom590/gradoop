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

package org.gradoop.flink.algorithms.fsm.cross_level.functions.preprocessing;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.common.comparison.DFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.common.comparison.DirectedDFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.common.comparison.UndirectedDFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConstants;
import org.gradoop.flink.algorithms.fsm.cross_level.config.CrossLevelTFSMConfig;
import org.gradoop.flink.algorithms.fsm.cross_level.model.GraphUtils;
import org.gradoop.flink.algorithms.fsm.cross_level.model.SearchGraphUtils;
import org.gradoop.flink.algorithms.fsm.cross_level.model.UnsortedSearchGraphUtils;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.EncodedMultilevelGraph;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Arrays;
import java.util.Map;

/**
 * Encodes edge labels to integers.
 * Drops edges with infrequent labels and isolated vertices.
 */
public class Encode extends RichMapFunction<GraphTransaction, EncodedMultilevelGraph> {

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
  public Encode(CrossLevelTFSMConfig fsmConfig) {
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
  public EncodedMultilevelGraph map(GraphTransaction inGraph) throws Exception {

    // VERTICES

    Map<GradoopId, Integer> vertexIdMap = Maps.newHashMap();
    int[] vertexTopLevels = new int[0];
    int[][] vertexLowerLevels = new int[0][];

    int id = 0;
    for (Vertex vertex : inGraph.getVertices()) {

      String label = vertex.getLabel();
      String[] stringLevels = label.contains(FSMConstants.DIMENSION_SEPARATOR) ?
        label.split(FSMConstants.DIMENSION_SEPARATOR) :
        new String[] {label};

      Integer intTopLevel = dictionary.get(stringLevels[0]);

      if (intTopLevel != null) {
        // vertex has frequent top level

        int depth = stringLevels.length;
        int[] intLevels = new int[0];

        for (int i = 1; i < depth; i++) {
          String stringLevel = stringLevels[i];
          Integer intLevel = dictionary.get(stringLevel);

          if (intLevel != null) {
            ArrayUtils.add(intLevels, intLevel);
          } else {
            break;
          }
        }

        vertexIdMap.put(vertex.getId(), id);
        vertexTopLevels = ArrayUtils.add(vertexTopLevels, intTopLevel);
        vertexLowerLevels = ArrayUtils.add(vertexLowerLevels, intLevels);

        id++;
      }
    }

    // EDGES

    int[][] dfsCodes = new int[0][];

    for (Edge edge : inGraph.getEdges()) {
      Integer edgeLabel = dictionary.get(edge.getLabel());

      if (edgeLabel != null) {
        // edge has frequent label

        Integer sourceId = vertexIdMap.get(edge.getSourceId());

        if (sourceId != null) {
          int sourceLabel = vertexTopLevels[sourceId];

          if (sourceId >= 0) {
            // source vertex has frequent label

            Integer targetId = vertexIdMap.get(edge.getTargetId());

            if (targetId != null) {
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
