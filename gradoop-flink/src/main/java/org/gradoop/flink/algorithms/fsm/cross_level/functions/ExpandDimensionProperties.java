package org.gradoop.flink.algorithms.fsm.cross_level.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConstants;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Collection;
import java.util.Map;

public class ExpandDimensionProperties implements MapFunction<GraphTransaction, GraphTransaction> {

  private final Map<String, Integer> labelDepth = Maps.newHashMap();

  @Override
  public GraphTransaction map(GraphTransaction graph) throws Exception {

    Collection<Vertex> dimVertices = Lists.newArrayList();

    for (Vertex vertex : graph.getVertices()) {

      Integer depth = labelDepth.get(vertex.getLabel());

      if (depth == null) {
        depth = 0;

        for (Property property : vertex.getProperties()) {
          if (property.getKey().startsWith(FSMConstants.DIMENSION_PREFIX)) {
            depth++;
          }
        }

        labelDepth.put(vertex.getLabel(), depth);

      }

      if (depth > 0) {
        GradoopId sourceId = vertex.getId();

        for (int i = 0; i < depth; i++) {
          GradoopId targetId = GradoopId.get();
          String edgeLabel = FSMConstants.DIMENSION_PREFIX + String.valueOf(i);
          graph.getEdges().add(new Edge(GradoopId.get(), edgeLabel, sourceId, targetId, null, null));

          String vertexLabel = vertex.getPropertyValue(edgeLabel).toString();
          dimVertices.add(new Vertex(targetId, vertexLabel, null, null));

          sourceId = targetId;
        }
      }
    }

    graph.getVertices().addAll(dimVertices);

    return graph;
  }
}
