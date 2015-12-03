package org.gradoop.model.impl.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.algorithms.fsm.functions.*;
import org.gradoop.model.impl.algorithms.fsm.tuples.CompressedDfsCode;
import org.gradoop.model.impl.algorithms.fsm.tuples.FSMEdge;
import org.gradoop.model.impl.algorithms.fsm.tuples.GSpanGraph;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.functions.join.LeftSide;
import org.gradoop.model.impl.functions.tuple.Project3To0And1;
import org.gradoop.model.impl.functions.tuple.Project2To0;
import org.gradoop.model.impl.functions.tuple.Project4To1And2And3;
import org.gradoop.model.impl.functions.tuple.Value0Of2;
import org.gradoop.model.impl.functions.tuple.Value0Of3;
import org.gradoop.model.impl.functions.tuple.Value1Of3;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.count.Count;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.Collection;

public class GSpan
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryCollectionToCollectionOperator<G, V, E> {

  private final float threshold;
  protected DataSet<Long> minCount;
  private GradoopFlinkConfig<G, V, E> config;

  public GSpan(float threshold) {
    this.threshold = threshold;
  }

  @Override
  public GraphCollection<G, V, E> execute(GraphCollection<G, V, E> collection)  {

    this.config = collection.getConfig();

    DataSet<GSpanGraph> searchSpace = createSearchSpace(collection);

    DataSet<CompressedDfsCode> frequentDfsCodes = searchSpace
      .flatMap(new ReportDfsCodes())
      .distinct();

    DataSet<Tuple3<G, Collection<V>, Collection<E>>> frequentSubgraphs =
      frequentDfsCodes
        .map(new DfsDecoder<>(
          config.getGraphHeadFactory(),
          config.getVertexFactory(),
          config.getEdgeFactory()
        ));


    return createResultCollection(frequentSubgraphs);
  }

  protected GraphCollection<G, V, E> createResultCollection(
    DataSet<Tuple3<G, Collection<V>, Collection<E>>> frequentSubgraphs) {

    DataSet<G> graphHeads = frequentSubgraphs
        .map(new Value0Of3<G, Collection<V>, Collection<E>>());

    DataSet<V> vertices = frequentSubgraphs
      .flatMap(new VertexExpander<G, V, E>())
      .returns(config.getVertexFactory().getType());

    DataSet<E> edges = frequentSubgraphs
      .flatMap(new EdgeExpander<G, V, E>())
      .returns(config.getEdgeFactory().getType());

    return GraphCollection.fromDataSets(graphHeads, vertices, edges, config);
  }


  protected void setMinCount(GraphCollection<G, V, E> collection) {
    this.minCount = Count
      .count(collection.getGraphHeads())
      .map(new MinCount(threshold));
  }

  protected DataSet<GSpanGraph> createSearchSpace(
    GraphCollection<G, V, E> collection) {

    setMinCount(collection);

    // VERTEX PRE PROCESSING

    //(f2 ? vertexId : graphId, vertexLabel, containsVertexId)
    DataSet<Tuple3<GradoopId, String, Boolean>> vertexLabelOccurrences =
      collection.getVertices()
      .flatMap(new VertexLabelMapper<V>());

    DataSet<Tuple1<String>> frequentVertexLabels = Count
      .groupBy(vertexLabelOccurrences
        .filter(new ContainsGraphId())
        .distinct()
        .map(new Value1Of3<GradoopId, String, Boolean>())
      )
      .filter(new Frequent<String>())
      .withBroadcastSet(minCount, Frequent.DS_NAME)
      .map(new Project2To0<String, Long>());

    // (id, label)
    DataSet<Tuple2<GradoopId, String>> vertices =
      vertexLabelOccurrences
        .filter(new ContainsVertexId())
        .map(new Project3To0And1<GradoopId, String, Boolean>())
        .join(frequentVertexLabels)
        .where(1).equalTo(0) // label == label
        .with(new LeftSide<Tuple2<GradoopId, String>, Tuple1<String>>());

    // EDGE PRE PROCESSING

    // (graphId, SourceId, sourceLabel, edgeIs, edgeLabel, targetId)
    DataSet<FSMEdge>
      edges = collection.getEdges()
      .join(vertices)
      .where(new SourceId<E>()).equalTo(0) // sourceId == id
      .with(new EdgeWithSourceLabelAsTuple<E>())
      .join(vertices)
      .where(5).equalTo(0) // targetId == id
      .with(new EdgeWithVertexLabelsAsTuple());

    // (sourceLabel, edgeLabel, targetLabel)
    DataSet<Tuple3<String, String, String>> frequentEdgeLabelTriples = Count
      .groupBy(edges
        .map(new DropEdgeAndVertexIds())
        .distinct()
        .map(new Project4To1And2And3<GradoopId, String, String, String>())
      )
      .filter(new Frequent<Tuple3<String, String, String>>())
      .withBroadcastSet(minCount, Frequent.DS_NAME)
      .map(new Value0Of2<Tuple3<String, String, String>, Long>());

    edges = edges
      .join(frequentEdgeLabelTriples)
      .where(2, 4, 6).equalTo(0, 1, 2)
      .with(new LeftSide<FSMEdge, Tuple3<String, String, String>>());

    return edges
      .groupBy(0) // graphId
      .reduceGroup(new GSpanGraphBuilder());
  }


  @Override
  public String getName() {
    return null;
  }
}
