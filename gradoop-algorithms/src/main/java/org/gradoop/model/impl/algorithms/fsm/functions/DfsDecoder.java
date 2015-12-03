package org.gradoop.model.impl.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMGraphHeadFactory;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.DfsCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.DfsStep;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class DfsDecoder
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements ResultTypeQueryable<Tuple3<G, Collection<V>, Collection<E>>>,
  MapFunction<CompressedDfsCode, Tuple3<G, Collection<V>, Collection<E>>> {

  private final EPGMGraphHeadFactory<G> graphHeadFactory;
  private final EPGMVertexFactory<V> vertexFactory;
  private final EPGMEdgeFactory<E> edgeFactory;

  public DfsDecoder(

    EPGMGraphHeadFactory<G> graphHeadFactory,
    EPGMVertexFactory<V> vertexFactory,
    EPGMEdgeFactory<E> edgeFactory) {

    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;

  }

  protected GradoopId getOrCreateVertex(short fromTime, String fromLabel,
    Collection<V> vertices, Map<Short, GradoopId> vertexTimeId,
    GradoopIdSet graphIds) {

    GradoopId fromId = vertexTimeId.get(fromTime);

    if (fromId == null) {
      V vertex = vertexFactory.createVertex(fromLabel, graphIds);

      fromId = vertex.getId();
      vertices.add(vertex);
      vertexTimeId.put(fromTime, fromId);
    }
    return fromId;
  }


  @Override
  public Tuple3<G, Collection<V>, Collection<E>> map(CompressedDfsCode
    compressedDfsCode)
    throws
    Exception {

    DfsCode dfsCode = compressedDfsCode.decompress();

    G graphHead = graphHeadFactory.createGraphHead(dfsCode.toString());

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());

    Collection<V> vertices = Lists
      .newArrayListWithCapacity(dfsCode.getVertexCount());

    Collection<E> edges = Lists
      .newArrayListWithCapacity(dfsCode.getEdgeCount());

    Map<Short, GradoopId> vertexTimeId = new HashMap<>();

    for(DfsStep step : dfsCode.getSteps()) {

      short fromTime = step.getFromTime();
      String fromLabel = step.getFromLabel();

      short toTime = step.getToTime();
      String toLabel = step.getToLabel();

      GradoopId targetId;
      GradoopId sourceId;

      if(step.isForward()) {
        sourceId = getOrCreateVertex(
          fromTime, fromLabel, vertices, vertexTimeId, graphIds);

        targetId = getOrCreateVertex(
          toTime, toLabel, vertices, vertexTimeId, graphIds);

      } else {
        sourceId = getOrCreateVertex(
          toTime, toLabel, vertices, vertexTimeId, graphIds);

        if(step.isLoop()) {
          targetId = sourceId;
        } else {
          targetId = getOrCreateVertex(
            fromTime, fromLabel, vertices, vertexTimeId, graphIds);
        }
      }

      edges.add(edgeFactory.createEdge(
        step.getEdgeLabel(), sourceId, targetId, graphIds));
    }

    return new Tuple3<>(graphHead, vertices, edges);
  }

  @Override
  public TypeInformation<Tuple3<G, Collection<V>, Collection<E>>>
  getProducedType() {
    return new TupleTypeInfo<>(
      TypeExtractor.getForClass(graphHeadFactory.getType()),
      TypeExtractor.getForClass(Collection.class),
      TypeExtractor.getForClass(Collection.class));
  }
}
