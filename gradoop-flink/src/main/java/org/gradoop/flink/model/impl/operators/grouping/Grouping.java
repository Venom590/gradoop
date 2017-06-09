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

package org.gradoop.flink.model.impl.operators.grouping;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.util.GConstants;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.functions.BuildEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.functions.CombineEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.ReduceEdgeGroupItems;
import org.gradoop.flink.model.impl.operators.grouping.functions.UpdateEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.EdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;



import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The grouping operator determines a structural grouping of vertices and edges
 * to condense a graph and thus help to uncover insights about patterns and
 * statistics hidden in the graph.
 * <p>
 * The graph grouping operator represents every vertex group by a single super
 * vertex in the resulting graph; (super) edges between vertices in the
 * resulting graph represent a group of edges between the vertex group members
 * of the original graph. Grouping is defined by specifying grouping keys for
 * vertices and edges, respectively, similarly as for GROUP BY in SQL.
 * <p>
 * Consider the following example:
 * <p>
 * Input graph:
 * <p>
 * Vertices:<br>
 * (0, "Person", {city: L})<br>
 * (1, "Person", {city: L})<br>
 * (2, "Person", {city: D})<br>
 * (3, "Person", {city: D})<br>
 * <p>
 * Edges:{(0,1), (1,0), (1,2), (2,1), (2,3), (3,2)}
 * <p>
 * Output graph (grouped on vertex property "city"):
 * <p>
 * Vertices:<br>
 * (0, "Person", {city: L, count: 2})
 * (2, "Person", {city: D, count: 2})
 * <p>
 * Edges:<br>
 * ((0, 0), {count: 2}) // 2 intra-edges in L<br>
 * ((2, 2), {count: 2}) // 2 intra-edges in L<br>
 * ((0, 2), {count: 1}) // 1 inter-edge from L to D<br>
 * ((2, 0), {count: 1}) // 1 inter-edge from D to L<br>
 * <p>
 * In addition to vertex properties, grouping is also possible on edge
 * properties, vertex- and edge labels as well as combinations of those.
 */
public abstract class Grouping implements UnaryGraphToGraphOperator {
  /**
   * Used as property key to declare a label based grouping.
   *
   * See {@link LogicalGraph#groupBy(List, List, List, List, GroupingStrategy)}
   */
  public static final String LABEL_SYMBOL = ":label";
  /**
   * Used to verify if an aggregator is used for all vertices.
   */
  public static final String VERTEX_AGGREGATOR = ":vertexAggregator";
  /**
   * Used to verify if a grouping key is used for all vertices.
   */
  public static final String DEFAULT_VERTEX_LABEL_GROUP = ":defaultVertexLabelGroup";
  /**
   * Used to verify if an aggregator is used for all edges.
   */
  public static final String EDGE_AGGREGATOR = ":edgeAggregator";
  /**
   * Used to verify if a grouping key is used for all edges.
   */
  public static final String DEFAULT_EDGE_LABEL_GROUP = ":defaultEdgeLabelGroup";
  /**
   * Gradoop Flink configuration.
   */
  protected GradoopFlinkConfig config;
//  /**
//   * Used to group vertices.
//   */
//  private final List<String> vertexGroupingKeys;
  /**
   * True if vertices shall be grouped using their label.
   */
  private final boolean useVertexLabels;
//  /**
//   * Aggregate functions which are applied on super vertices.
//   */
//  private final List<PropertyValueAggregator> vertexAggregators;
//  /**
//   * Used to group edges.
//   */
//  private final List<String> edgeGroupingKeys;
  /**
   * True if edges shall be grouped using their label.
   */
  private final boolean useEdgeLabels;
//  /**
//   * Aggregate functions which are applied on super edges.
//   */
//  private final List<PropertyValueAggregator> edgeAggregators;

  /**
   * Stores grouping properties and aggregators for vertex labels.
   */
  private final List<LabelGroup> vertexLabelGroups;

  /**
   * Stores grouping properties and aggregators for edge labels.
   */
  private final List<LabelGroup> edgeLabelGroups;

//  /**
//   * Stores all aggregator property keys for each label.
//   */
//  private Map<String, Set<String>> labelWithAggregatorPropertyKeys;

  /**
   * Creates grouping operator instance.
   *
//   * @param vertexGroupingKeys              property keys to group vertices
   * @param useVertexLabels                 group on vertex label true/false
//   * @param vertexAggregators               aggregate functions for grouped vertices
//   * @param edgeGroupingKeys                property keys to group edges
   * @param useEdgeLabels                   group on edge label true/false
//   * @param edgeAggregators                 aggregate functions for grouped edges
   * @param vertexLabelGroups               stores grouping properties for vertex labels
   * @param edgeLabelGroups                 stores grouping properties for edge labels
//   * @param labelWithAggregatorPropertyKeys stores all aggregator property keys for each label.
   */
  Grouping(
    boolean useVertexLabels,
    boolean useEdgeLabels,
    List<LabelGroup> vertexLabelGroups,
    List<LabelGroup> edgeLabelGroups) {
//  Grouping(
//    List<String> vertexGroupingKeys, boolean useVertexLabels,
//    List<PropertyValueAggregator> vertexAggregators,
//    List<String> edgeGroupingKeys, boolean useEdgeLabels,
//    List<PropertyValueAggregator> edgeAggregators,
//    List<LabelGroup> vertexLabelGroups,
//    List<LabelGroup> edgeLabelGroups,
//    Map<String, Set<String>> labelWithAggregatorPropertyKeys) {
//    this.vertexGroupingKeys              = vertexGroupingKeys;
    this.useVertexLabels                 = useVertexLabels;
//    this.vertexAggregators               = vertexAggregators;
//    this.edgeGroupingKeys                = edgeGroupingKeys;
    this.useEdgeLabels                   = useEdgeLabels;
//    this.edgeAggregators                 = edgeAggregators;
    this.vertexLabelGroups               = vertexLabelGroups;
    this.edgeLabelGroups                 = edgeLabelGroups;
//    this.labelWithAggregatorPropertyKeys = labelWithAggregatorPropertyKeys;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    LogicalGraph result;

    config = graph.getConfig();

    if (!useVertexProperties() &&
      !useEdgeProperties() &&
      !useVertexLabels() &&
      !useEdgeLabels()) {
      result = graph;
    } else {
      result = groupInternal(graph);
    }
    return result;
  }

  /**
   * Returns true if vertex properties shall be used for grouping.
   *
   * @return true iff vertex properties shall be used for grouping
   */
  protected boolean useVertexProperties() {
    return !vertexLabelGroups.isEmpty();
//    return !getVertexGroupingKeys().isEmpty() || !vertexLabelGroups.isEmpty();
  }

//  /**
//   * Returns vertex property keys which are used for grouping vertices.
//   *
//   * @return vertex property keys
//   */
//  protected List<String> getVertexGroupingKeys() {
//    return vertexGroupingKeys;
//  }

  /**
   * True, iff vertex labels shall be used for grouping.
   *
   * @return true, iff vertex labels shall be used for grouping
   */
  protected boolean useVertexLabels() {
    return useVertexLabels;
  }

//  /**
//   * Returns the aggregate functions which are applied on super vertices.
//   *
//   * @return vertex aggregate functions
//   */
//  protected List<PropertyValueAggregator> getVertexAggregators() {
//    return vertexAggregators;
//  }

  /**
   * Returns true if edge properties shall be used for grouping.
   *
   * @return true, iff edge properties shall be used for grouping
   */
  protected boolean useEdgeProperties() {
    return !edgeLabelGroups.isEmpty();
//    return !edgeGroupingKeys.isEmpty() || !edgeLabelGroups.isEmpty();
  }

//  /**
//   * Returns edge property keys which are used for grouping edges.
//   *
//   * @return edge property keys
//   */
//  protected List<String> getEdgeGroupingKeys() {
//    return edgeGroupingKeys;
//  }

  /**
   * True, if edge labels shall be used for grouping.
   *
   * @return true, iff edge labels shall be used for grouping
   */
  protected boolean useEdgeLabels() {
    return useEdgeLabels;
  }

//  /**
//   * Returns the aggregate functions which shall be applied on super edges.
//   *
//   * @return edge aggregate functions
//   */
//  protected List<PropertyValueAggregator> getEdgeAggregators() {
//    return edgeAggregators;
//  }

  /**
   * Returns tuple which contains the properties used for a specific vertex label.
   *
   * @return vertex label groups
   */
  public List<LabelGroup> getVertexLabelGroups() {
    return vertexLabelGroups;
  }

  /**
   * Returns tuple which contains the properties used for a specific edge label.
   *
   * @return edge label groups
   */
  public List<LabelGroup> getEdgeLabelGroups() {
    return edgeLabelGroups;
  }

//  /**
//   * Returns the aggregator property keys for each label.
//   *
//   * @return map from label to set of aggregator property keys
//   */
//  protected Map<String, Set<String>> getLabelWithAggregatorPropertyKeys() {
//    return labelWithAggregatorPropertyKeys;
//  }

  /**
   * Group vertices by either vertex label, vertex property or both.
   *
   * @param groupVertices dataset containing vertex representation for grouping
   * @return unsorted vertex grouping
   */
  protected UnsortedGrouping<VertexGroupItem> groupVertices(
    DataSet<VertexGroupItem> groupVertices) {
    UnsortedGrouping<VertexGroupItem> vertexGrouping;
    if (useVertexLabels() && useVertexProperties()) {
      vertexGrouping = groupVertices.groupBy(2, 3);
    } else if (useVertexLabels()) {
      vertexGrouping = groupVertices.groupBy(2);
    } else {
      vertexGrouping = groupVertices.groupBy(3);
    }
    return vertexGrouping;
  }

  /**
   * Groups edges based on the algorithm parameters.
   *
   * @param edges input graph edges
   * @return grouped edges
   */
  protected UnsortedGrouping<EdgeGroupItem> groupEdges(DataSet<EdgeGroupItem> edges) {
    UnsortedGrouping<EdgeGroupItem> groupedEdges;
    if (useEdgeProperties() && useEdgeLabels()) {
      groupedEdges = edges.groupBy(0, 1, 2, 3);
    } else if (useEdgeLabels()) {
      groupedEdges = edges.groupBy(0, 1, 2);
    } else if (useEdgeProperties()) {
      groupedEdges = edges.groupBy(0, 1, 3);
    } else {
      groupedEdges = edges.groupBy(0, 1);
    }
    return groupedEdges;
  }

  /**
   * Build super edges by joining them with vertices and their super vertex.
   *
   * @param graph                     input graph
   * @param vertexToRepresentativeMap dataset containing tuples of vertex id
   *                                  and super vertex id
   * @return super edges
   */
  protected DataSet<Edge> buildSuperEdges(
    LogicalGraph graph,
    DataSet<VertexWithSuperVertex> vertexToRepresentativeMap) {

    DataSet<EdgeGroupItem> edges = graph.getEdges()
      // build edge group items
      .flatMap(new BuildEdgeGroupItem(useEdgeLabels(), getEdgeLabelGroups()))
//      .flatMap(new BuildEdgeGroupItem(
//        getEdgeGroupingKeys(), useEdgeLabels(), getEdgeAggregators(), getEdgeLabelGroups(),
//        getLabelWithAggregatorPropertyKeys()))
      // join edges with vertex-group-map on source-id == vertex-id
      .join(vertexToRepresentativeMap)
      .where(0).equalTo(0)
      .with(new UpdateEdgeGroupItem(0))
      .withForwardedFieldsFirst("f1;f2;f3;f4")
      .withForwardedFieldsSecond("f1->f0")
      // join result with vertex-group-map on target-id == vertex-id
      .join(vertexToRepresentativeMap)
      .where(1).equalTo(0)
      .with(new UpdateEdgeGroupItem(1))
      .withForwardedFieldsFirst("f0;f2;f3;f4")
      .withForwardedFieldsSecond("f1->f1");

    // group + combine
    DataSet<EdgeGroupItem> combinedEdges = groupEdges(edges)
      .combineGroup(new CombineEdgeGroupItems(useEdgeLabels()));
//      .combineGroup(new CombineEdgeGroupItems(
//        getEdgeGroupingKeys(), useEdgeLabels(), getEdgeAggregators(),
//        getLabelWithAggregatorPropertyKeys()));

    // group + reduce + build final edges
    return groupEdges(combinedEdges)
      .reduceGroup(new ReduceEdgeGroupItems(
        useEdgeLabels(),
        config.getEdgeFactory()));
//      .reduceGroup(new ReduceEdgeGroupItems(getEdgeGroupingKeys(),
//        useEdgeLabels(),
//        getEdgeAggregators(),
//        config.getEdgeFactory(),
//        getLabelWithAggregatorPropertyKeys()));
  }

  /**
   * Overridden by concrete implementations.
   *
   * @param graph input graphe
   * @return grouped output graph
   */
  protected abstract LogicalGraph groupInternal(LogicalGraph graph);

  /**
   * Used for building a grouping operator instance.
   */
  public static final class GroupingBuilder {

    /**
     * Grouping strategy
     */
    private GroupingStrategy strategy;

//    /**
//     * Property keys to group vertices.
//     */
//    private List<String> vertexGroupingKeys;

//    /**
//     * Property keys to group edges.
//     */
//    private List<String> edgeGroupingKeys;

    /**
     * True, iff vertex labels shall be considered.
     */
    private boolean useVertexLabel;

    /**
     * True, iff edge labels shall be considered.
     */
    private boolean useEdgeLabel;

//    /**
//     * Aggregate functions which will be applied on vertex properties.
//     */
//    private List<PropertyValueAggregator> vertexValueAggregators;

//    /**
//     * Aggregate functions which will be applied on edge properties.
//     */
//    private List<PropertyValueAggregator> edgeValueAggregators;

    /**
     * Stores grouping keys for a specific vertex label.
     */
    private List<LabelGroup> vertexLabelGroups;

    /**
     * Stores grouping keys for a specific edge label.
     */
    private List<LabelGroup> edgeLabelGroups;

//    /**
//     * Stores all aggregator property keys for each label.
//     */
//    private Map<String, Set<String>> labelWithAggregatorPropertyKeys;

    /**
     * Creates a new grouping builder
     */
    public GroupingBuilder() {
//      this.vertexGroupingKeys     = new ArrayList<>();
//      this.edgeGroupingKeys       = new ArrayList<>();
      this.useVertexLabel         = false;
      this.useEdgeLabel           = false;
//      this.vertexValueAggregators = new ArrayList<>();
//      this.edgeValueAggregators   = new ArrayList<>();
      this.vertexLabelGroups      = Lists.newArrayList();
      this.edgeLabelGroups        = Lists.newArrayList();
//      this.labelWithAggregatorPropertyKeys = new HashMap<>();
    }

    /**
     * Set the grouping strategy. See {@link GroupingStrategy}.
     *
     * @param strategy grouping strategy
     * @return this builder
     */
    public GroupingBuilder setStrategy(GroupingStrategy strategy) {
      Objects.requireNonNull(strategy);
      this.strategy = strategy;
      return this;
    }

    /**
     * Adds a property key to the vertex grouping keys.
     *
     * @param key property key
     * @return this builder
     */
    public GroupingBuilder addVertexGroupingKey(String key) {
      Objects.requireNonNull(key);
      Set<LabelGroup> vertexLabelGroupSet;
      LabelGroup vertexLabelGroup;
      if (key.equals(Grouping.LABEL_SYMBOL)) {
        useVertexLabel(true);
      } else {
       getOrCreateDefaultVertexLabelGroup().addPropertyKey(key);
      }
      return this;
    }

    /**
     * Adds a list of property keys to the vertex grouping keys.
     *
     * @param keys property keys
     * @return this builder
     */
    public GroupingBuilder addVertexGroupingKeys(List<String> keys) {
      Objects.requireNonNull(keys);
      for (String key : keys) {
        this.addVertexGroupingKey(key);
      }
      return this;
    }

    /**
     * Adds a property key to the edge grouping keys.
     *
     * @param key property key
     * @return this builder
     */
    public GroupingBuilder addEdgeGroupingKey(String key) {
      Objects.requireNonNull(key);
      LabelGroup edgeLabelGroup;
        if (key.equals(Grouping.LABEL_SYMBOL)) {
          useEdgeLabel(true);
        } else {
          getOrCreateDefaultEdgeLabelGroup().addPropertyKey(key);
        }
      return this;
    }

    /**
     * Adds a list of property keys to the edge grouping keys.
     *
     * @param keys property keys
     * @return this builder
     */
    public GroupingBuilder addEdgeGroupingKeys(List<String> keys) {
      Objects.requireNonNull(keys);
      for (String key : keys) {
        this.addEdgeGroupingKey(key);
      }
      return this;
    }

    /**
     * Adds a vertex label group which defines the grouping keys for a specific label.
     * Note that a label may be used multiple times.
     *
     * @param vertexLabel vertex label
     * @param groupingKeys keys used for grouping
     * @return this builder
     */
    public GroupingBuilder addVertexLabelGroup(
      String groupLabel, String vertexLabel,
      List<String> groupingKeys) {
      return addVertexLabelGroup(groupLabel, vertexLabel, groupingKeys, Arrays.asList());
    }

    /**
     * Adds a vertex label group which defines the grouping keys and the aggregators for a
     * specific label. Note that a label may be used multiple times.
     *
     * @param vertexLabel vertex label
     * @param groupingKeys keys used for grouping
     * @param aggregators vertex aggregators
     * @return this builder
     */
    public GroupingBuilder addVertexLabelGroup(
      String groupLabel, String vertexLabel,
      List<String> groupingKeys,
      List<PropertyValueAggregator> aggregators) {
      vertexLabelGroups.add(new LabelGroup(groupLabel, vertexLabel, groupingKeys, aggregators));
      return this;
    }

    /**
     * Adds a vertex label group which defines the grouping keys and the aggregators for a
     * specific label. Note that a label may be used multiple times.
     *
     * @param vertexLabel vertex label
     * @param groupingKeys keys used for grouping
     * @param aggregators vertex aggregators
     * @return this builder
     */
    public GroupingBuilder addEdgeLabelGroup(
      String groupLabel, String vertexLabel,
      List<String> groupingKeys,
      List<PropertyValueAggregator> aggregators) {
      edgeLabelGroups.add(new LabelGroup(groupLabel, vertexLabel, groupingKeys, aggregators));
      return this;
    }

    /**
     * Define, if the vertex label shall be used for grouping vertices.
     *
     * @param useVertexLabel true, iff vertex label shall be used for grouping
     * @return this builder
     */
    public GroupingBuilder useVertexLabel(boolean useVertexLabel) {
      this.useVertexLabel = useVertexLabel;
      return this;
    }

    /**
     * Define, if the edge label shall be used for grouping edges.
     *
     * @param useEdgeLabel true, iff edge label shall be used for grouping
     * @return this builder
     */
    public GroupingBuilder useEdgeLabel(boolean useEdgeLabel) {
      this.useEdgeLabel = useEdgeLabel;
      return this;
    }

    /**
     * Add an aggregate function which is applied on a group of vertices
     * represented by a single super vertex.
     *
     * @param aggregator vertex aggregator
     * @return this builder
     */
    public GroupingBuilder addVertexAggregator(PropertyValueAggregator aggregator) {
      Objects.requireNonNull(aggregator, "Aggregator must not be null");
      getOrCreateDefaultVertexLabelGroup().addAggregator(aggregator);
      return this;
    }

    /**
     * Add an aggregate function which is applied on a group of edges
     * represented by a single super edge.
     *
     * @param aggregator edge aggregator
     * @return this builder
     */
    public GroupingBuilder addEdgeAggregator(PropertyValueAggregator aggregator) {
      Objects.requireNonNull(aggregator, "Aggregator must not be null");
      getOrCreateDefaultEdgeLabelGroup().addAggregator(aggregator);
      return this;
    }

    /**
     * Creates a new grouping operator instance based on the configured
     * parameters.
     *
     * @return grouping operator instance
     */
    public Grouping build() {
      if (vertexLabelGroups.isEmpty() && !useVertexLabel) {
//      if (vertexGroupingKeys.isEmpty() && !useVertexLabel) {
        throw new IllegalArgumentException(
          "Provide vertex key(s) and/or use vertex labels for grouping.");
      }

      if (vertexLabelGroups.isEmpty()) {
        getOrCreateDefaultVertexLabelGroup();
      }
      if (edgeLabelGroups.isEmpty()) {
        getOrCreateDefaultEdgeLabelGroup();
      }

      Grouping groupingOperator;

      switch (strategy) {
      case GROUP_REDUCE:
        groupingOperator = new GroupingGroupReduce(
          useVertexLabel, useEdgeLabel, vertexLabelGroups, edgeLabelGroups);
//        groupingOperator = new GroupingGroupReduce(
//          vertexGroupingKeys, useVertexLabel, vertexValueAggregators, edgeGroupingKeys,
//          useEdgeLabel, edgeValueAggregators, vertexLabelGroups, edgeLabelGroups,
//          labelWithAggregatorPropertyKeys);
        break;
      case GROUP_COMBINE:
        groupingOperator = new GroupingGroupCombine(
          useVertexLabel, useEdgeLabel, vertexLabelGroups, edgeLabelGroups);
        break;
      default:
        throw new IllegalArgumentException("Unsupported strategy: " + strategy);
      }

      return groupingOperator;
    }

    /**
     * Returns the label group which is used for grouping all vertices.
     *
     * @return default label group
     */
    private LabelGroup getOrCreateDefaultVertexLabelGroup() {
      LabelGroup defaultVertexLabelGroup;
      for (LabelGroup vertexLabelGroup : vertexLabelGroups) {
        if (vertexLabelGroup.getGroupingLabel().equals(Grouping.DEFAULT_VERTEX_LABEL_GROUP)) {
          return vertexLabelGroup;
        }
      }
      defaultVertexLabelGroup = new LabelGroup(GConstants.DEFAULT_VERTEX_LABEL, Grouping
        .DEFAULT_VERTEX_LABEL_GROUP);
      vertexLabelGroups.add(defaultVertexLabelGroup);
      return defaultVertexLabelGroup;
    }

    /**
     * Returns the label group which is used for grouping all edges.
     *
     * @return default label group
     */
    private LabelGroup getOrCreateDefaultEdgeLabelGroup() {
      LabelGroup defaultEdgeLabelGroup;
      for (LabelGroup edgeLabelGroup : edgeLabelGroups) {
        if (edgeLabelGroup.getGroupingLabel().equals(Grouping.DEFAULT_EDGE_LABEL_GROUP)) {
          return edgeLabelGroup;
        }
      }
      defaultEdgeLabelGroup = new LabelGroup(GConstants.DEFAULT_EDGE_LABEL, Grouping.DEFAULT_EDGE_LABEL_GROUP);
      edgeLabelGroups.add(defaultEdgeLabelGroup);
      return defaultEdgeLabelGroup;
    }
  }
}
