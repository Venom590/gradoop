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

package org.gradoop.flink.algorithms.fsm.cross_level.functions.conversion;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConstants;
import org.gradoop.flink.algorithms.fsm.cross_level.model.GraphUtils;
import org.gradoop.flink.algorithms.fsm.cross_level.model.GraphUtilsBase;
import org.gradoop.flink.algorithms.fsm.cross_level.model.Simple16Compressor;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.MultilevelGraph;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.List;
import java.util.Set;

/**
 * int-array encoded graph => Gradoop Graph Transaction
 */
public class MultilevelPatternToEPGMGraphTransaction
  extends RichMapFunction<MultilevelGraph, GraphTransaction> {


  /**
   * frequent vertex labels
   */
  private String[] vertexDictionary;

  /**
   * frequent vertex labels
   */
  private String[] edgeDictionary;

  /**
   * frequent vertex labels
   */
  private String[] levelDictionary;


  /**
   * utils to interpret and manipulate integer encoded graphs
   */
  private final GraphUtils graphUtils = new GraphUtilsBase();

  /**
   * Input graph collection size
   */
  private long graphCount;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    vertexDictionary = getRuntimeContext()
      .<String[]>getBroadcastVariable(FSMConstants.VERTEX_DICTIONARY).get(0);

    edgeDictionary = getRuntimeContext()
      .<String[]>getBroadcastVariable(FSMConstants.EDGE_DICTIONARY).get(0);

    List<String[]> broadcast =
      getRuntimeContext().<String[]>getBroadcastVariable(FSMConstants.LEVEL_DICTIONARY);

    levelDictionary = broadcast.size() == 0 ? new String[0] : broadcast.get(0);

    graphCount = getRuntimeContext()
      .<Long>getBroadcastVariable(FSMConstants.GRAPH_COUNT).get(0);
  }

  @Override
  public GraphTransaction map(MultilevelGraph multilevelPattern) throws Exception {

    int[] pattern = multilevelPattern.getGraph();

    int[][] vector = multilevelPattern.getVector();

    pattern = Simple16Compressor.uncompress(pattern);


    long frequency = 0;


    // GRAPH HEAD
    GraphHead graphHead = new GraphHead(GradoopId.get(), "", null);

//    graphHead.setLabel(FSMConstants.FREQUENT_PATTERN_LABEL);
//    graphHead.setProperty(FSMConstants.SUPPORT_KEY, (float) frequency / graphCount);

    GradoopIdList graphIds = GradoopIdList.fromExisting(graphHead.getId());

    // VERTICES
    int[] vertexLabels = graphUtils.getVertexLabels(pattern);

    GradoopId[] vertexIds = new GradoopId[vertexLabels.length];

    Set<Vertex> vertices = Sets.newHashSetWithExpectedSize(vertexLabels.length);

    int intId = 0;
    for (int intLabel : vertexLabels) {
      String label = vertexDictionary[intLabel];

      GradoopId gradoopId = GradoopId.get();
      Vertex vertex = new Vertex(gradoopId, label, null, graphIds);

      int[] dimension = vector[intId];

      for (int level = 0; level < dimension.length; level++) {
        int value = dimension[level];

        if (value == 0) {
          break;
        } else {
          vertex.setProperty(
            FSMConstants.DIMENSION_PREFIX + String.valueOf(level), levelDictionary[value]);
        }
      }

      vertices.add(vertex);

      vertexIds[intId] = gradoopId;
      intId++;
    }

    // EDGES
    Set<Edge> edges = Sets.newHashSet();

    for (int edgeId = 0; edgeId < graphUtils.getEdgeCount(pattern); edgeId++) {
      String label = edgeDictionary[graphUtils.getEdgeLabel(pattern, edgeId)];

      GradoopId sourceId;
      GradoopId targetId;

      if (graphUtils.isOutgoing(pattern, edgeId)) {
        sourceId = vertexIds[graphUtils.getFromId(pattern, edgeId)];
        targetId = vertexIds[graphUtils.getToId(pattern, edgeId)];
      } else {
        sourceId = vertexIds[graphUtils.getToId(pattern, edgeId)];
        targetId = vertexIds[graphUtils.getFromId(pattern, edgeId)];
      }

      edges.add(new Edge(GradoopId.get(), label, sourceId, targetId, null, graphIds));
    }

    GraphTransaction graphTransaction = new GraphTransaction(graphHead, vertices, edges);

    return graphTransaction;
  }
}
