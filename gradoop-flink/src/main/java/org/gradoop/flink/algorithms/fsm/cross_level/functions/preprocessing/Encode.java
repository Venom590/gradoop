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
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.algorithms.fsm.common.comparison.DFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.common.comparison.DirectedDFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.common.comparison.UndirectedDFSBranchComparator;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConstants;
import org.gradoop.flink.algorithms.fsm.cross_level.config.CrossLevelTFSMConfig;
import org.gradoop.flink.algorithms.fsm.cross_level.model.GraphUtils;
import org.gradoop.flink.algorithms.fsm.cross_level.model.SearchGraphUtils;
import org.gradoop.flink.algorithms.fsm.cross_level.model.UnsortedSearchGraphUtils;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.MultilevelGraph;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Encodes edge labels to integers.
 * Drops edges with infrequent labels and isolated vertices.
 */
public class Encode extends RichMapFunction<GraphTransaction, MultilevelGraph> {

  /**
   * vertex label dictionary
   */
  private Map<String, Integer> vertexDictionary = Maps.newHashMap();

  /**
   * vertex label dictionary
   */
  private Map<String, Integer> edgeDictionary = Maps.newHashMap();

  /**
   * vertex label dictionary
   */
  private Map<String, Integer> levelDictionary = Maps.newHashMap();

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

  private final Map<Integer, Integer> labelDepth = Maps.newHashMap();

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
    String[] array = getRuntimeContext()
      .<String[]>getBroadcastVariable(FSMConstants.VERTEX_DICTIONARY).get(0);

    for (int i = 0; i < array.length; i++) {
      vertexDictionary.put(array[i], i);
    }

    // create inverse dictionary at broadcast reception
    array = getRuntimeContext()
      .<String[]>getBroadcastVariable(FSMConstants.EDGE_DICTIONARY).get(0);

    for (int i = 0; i < array.length; i++) {
      edgeDictionary.put(array[i], i);
    }

    // create inverse dictionary at broadcast reception
    List<String[]> levelBroadcast =
      getRuntimeContext().getBroadcastVariable(FSMConstants.LEVEL_DICTIONARY);

    if (levelBroadcast.size() > 0) {
      array = levelBroadcast.get(0);

      for (int i = 0; i < array.length; i++) {
        levelDictionary.put(array[i], i);
      }
    }
  }

  @Override
  public MultilevelGraph map(GraphTransaction inGraph) throws Exception {

    // VERTICES

    Map<GradoopId, Integer> vertexIdMap = Maps.newHashMap();
    int[] vertexLabels = new int[0];
    int[][] vertexLevels = new int[0][];

    int id = 0;
    for (Vertex vertex : inGraph.getVertices()) {
      Integer label = vertexDictionary.get(vertex.getLabel());

      if (label != null) {
        // vertex has frequent top level

        Integer depth = labelDepth.get(label);

        if (depth == null) {
          depth = 0;

          for (Property property : vertex.getProperties()) {
            if (property.getKey().startsWith(FSMConstants.LEVEL_PREFIX)) {
              depth++;
            }
          }

          labelDepth.put(label, depth);
        }

        int[] levels = new int[depth];

        if (depth > 0) {
          for (Property property : vertex.getProperties()) {
            String key = property.getKey();

            if (key.startsWith(FSMConstants.LEVEL_PREFIX)) {
              int level = Integer.valueOf(key.substring(FSMConstants.LEVEL_PREFIX.length()));

              levels[level] = levelDictionary.get(property.getValue().toString());
            }
          }
        }

        vertexIdMap.put(vertex.getId(), id);
        vertexLabels = ArrayUtils.add(vertexLabels, label);
        vertexLevels = ArrayUtils.add(vertexLevels, levels);

        id++;
      }
    }

    // EDGES

    int[][] dfsCodes = new int[0][];

    for (Edge edge : inGraph.getEdges()) {
      Integer edgeLabel = edgeDictionary.get(edge.getLabel());

      if (edgeLabel != null) {
        // edge has frequent label

        Integer sourceId = vertexIdMap.get(edge.getSourceId());

        if (sourceId != null) {
          int sourceLabel = vertexLabels[sourceId];

          if (sourceId >= 0) {
            // source vertex has frequent label

            Integer targetId = vertexIdMap.get(edge.getTargetId());

            if (targetId != null) {
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

    return new MultilevelGraph(outGraph, vertexLevels);
  }
}
