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

package org.gradoop.flink.algorithms.fsm;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConstants;
import org.gradoop.flink.algorithms.fsm.common.config.DictionaryType;
import org.gradoop.flink.algorithms.fsm.common.comparison.AlphabeticalLabelComparator;
import org.gradoop.flink.algorithms.fsm.common.comparison.InverseProportionalLabelComparator;
import org.gradoop.flink.algorithms.fsm.common.comparison.LabelComparator;
import org.gradoop.flink.algorithms.fsm.common.comparison.ProportionalLabelComparator;
import org.gradoop.flink.algorithms.fsm.cross_level.config.CrossLevelTFSMConfig;
import org.gradoop.flink.algorithms.fsm.cross_level.config.VectorMiningStrategy;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.conversion.MultilevelPatternToEPGMGraphTransaction;

import org.gradoop.flink.algorithms.fsm.cross_level.functions.mining.CombineVectors;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.mining.ExpandFrequentPatterns;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.mining.Frequent;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.mining.FrequentCrossLevelPatterns;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.mining.GrowFrequentPatterns;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.mining.CreateCollector;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.mining.InitSingleEdgePatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.mining.IsFrequentPatternCollector;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.mining.ReportSupportedPatterns;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.mining.NotObsolete;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.preprocessing.CreateDictionary;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.preprocessing.Encode;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.preprocessing
  .InsertDummyZeroIntoDictionary;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.preprocessing.MinFrequency;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.preprocessing.NotEmpty;
import org.gradoop.flink.algorithms.fsm.cross_level.functions.preprocessing.ReportLabels;
import org.gradoop.flink.algorithms.fsm.common.gspan.DirectedGSpanLogic;
import org.gradoop.flink.algorithms.fsm.common.gspan.GSpanLogic;
import org.gradoop.flink.algorithms.fsm.common.gspan.UndirectedGSpanLogic;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.MultilevelGraph;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.MultilevelGraphWithPatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.cross_level.tuples.PatternVectors;
import org.gradoop.flink.algorithms.fsm.cross_level.vector_mining.CrossLevelFrequentVectors;
import org.gradoop.flink.algorithms.fsm.cross_level.vector_mining.CrossLevelFrequentVectorsBottomUp;
import org.gradoop.flink.algorithms.fsm.cross_level.vector_mining.CrossLevelFrequentVectorsTopDown;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of3;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of3;
import org.gradoop.flink.model.impl.functions.tuple.Value2Of3;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.util.ExpandArray;
import org.gradoop.flink.util.MakeCountable;

/**
 * Gradoop operator wrapping the DIMSpan algorithm for transactional frequent subgraph mining.
 */
public class CrossLevelTFSM implements UnaryCollectionToCollectionOperator {

  /**
   * Maximum number of iterations if set of k-edge frequent patterns is not running empty before.
   */
  private static final int MAX_ITERATIONS = 100;

  /**
   * FSM configuration
   */
  protected final CrossLevelTFSMConfig fsmConfig;

  /**
   * input graph collection cardinality
   */
  protected DataSet<Long> graphCount;

  /**
   * minimum frequency for patterns to be considered to be frequent
   */
  protected DataSet<Long> minFrequency;

  /**
   * Pattern growth and verification logic derived from gSpan.
   * See <a href="https://www.cs.ucsb.edu/~xyan/software/gSpan.htm">gSpan</a>
   */
  protected final GSpanLogic gSpan;

  private final CrossLevelFrequentVectors vectorMiner;

  /**
   * Vertex label dictionary for dictionary coding.
   */
  private DataSet<String[]> vertexLabelDictionary;

  /**
   * Vertex label dictionary for dictionary coding.
   */
  private DataSet<String[]> edgeLabelDictionary;

  /**
   * Vertex label dictionary for dictionary coding.
   */
  private DataSet<String[]> levelDictionary;

  /**
   * Label comparator used for dictionary coding.
   */
  private final LabelComparator comparator;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public CrossLevelTFSM(CrossLevelTFSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;

    // set gSpan implementation depending on direction mode
    gSpan = fsmConfig.isDirected() ?
      new DirectedGSpanLogic(fsmConfig) :
      new UndirectedGSpanLogic(fsmConfig);

    // set comparator based on dictionary type
    if (fsmConfig.getDictionaryType() == DictionaryType.PROPORTIONAL) {
      comparator =  new ProportionalLabelComparator();
    } else if (fsmConfig.getDictionaryType() == DictionaryType.INVERSE_PROPORTIONAL) {
      comparator =  new InverseProportionalLabelComparator();
    } else {
      comparator = new AlphabeticalLabelComparator();
    }

    vectorMiner = fsmConfig.getVectorMiningStrategy().equals(VectorMiningStrategy.TOP_DOWN) ?
      new CrossLevelFrequentVectorsTopDown() :
      new CrossLevelFrequentVectorsBottomUp();
  }

  /**
   * Constructor.
   *
   * @param minSupport minimum support threshold
   */
  public CrossLevelTFSM(float minSupport) {
    this(new CrossLevelTFSMConfig(minSupport, true));
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {

    // convert Gradoop graph collection to DIMSpan input format
    DataSet<GraphTransaction> input = collection
      .toTransactions()
      .getTransactions();

    // run DIMSpan
    DataSet<GraphTransaction> output = execute(input);

    // convert to Gradoop graph collection
    return GraphCollection
      .fromTransactions(new GraphTransactions(output, collection.getConfig()));
  }


  /**
   * Executes the DIMSpan algorithm.
   * Orchestration of preprocessing, mining and postprocessing.
   *
   * @param input input graph collection
   * @return frequent patterns
   */
  public DataSet<GraphTransaction> execute(DataSet<GraphTransaction> input) {

    DataSet<MultilevelGraph> encodedInput = preProcess(input);
    DataSet<MultilevelGraph> encodedOutput = mine(encodedInput);

    return postProcess(encodedOutput);
  }

  /**
   * Triggers the label-frequency base preprocessing
   *
   * @param graphs input
   * @return preprocessed input
   */
  private DataSet<MultilevelGraph> preProcess(DataSet<GraphTransaction> graphs) {

    // Execute vertex label pruning and dictionary coding
    createDictionaries(graphs);

    // Execute edge label pruning and dictionary coding
    DataSet<MultilevelGraph> encodedGraphs = graphs
      .map(new Encode(fsmConfig))
      .withBroadcastSet(vertexLabelDictionary, FSMConstants.VERTEX_DICTIONARY)
      .withBroadcastSet(edgeLabelDictionary, FSMConstants.EDGE_DICTIONARY)
      .withBroadcastSet(levelDictionary, FSMConstants.LEVEL_DICTIONARY);

    // return all non-obsolete encoded graphs
    return encodedGraphs
      .filter(new NotEmpty());
  }

  /**
   * Triggers the iterative mining process.
   *
   * @param graphs preprocessed input graph collection
   * @return frequent patterns
   */
  protected DataSet<MultilevelGraph> mine(DataSet<MultilevelGraph> graphs) {

    DataSet<MultilevelGraphWithPatternEmbeddingsMap> searchSpace = graphs
      .map(new InitSingleEdgePatternEmbeddingsMap(gSpan));

    // Workaround to support multiple data sinks: create pseudo-graph (collector),
    // which embedding map will be used to union all k-edge frequent patterns
    DataSet<MultilevelGraphWithPatternEmbeddingsMap> collector = graphs
      .getExecutionEnvironment()
      .fromElements(true)
      .map(new CreateCollector());

    searchSpace = searchSpace.union(collector);

    // ITERATION HEAD

    IterativeDataSet<MultilevelGraphWithPatternEmbeddingsMap> iterative = searchSpace
      .iterate(MAX_ITERATIONS);

    // ITERATION BODY

    DataSet<MultilevelGraph> reports = iterative
      .flatMap(new ReportSupportedPatterns());

    DataSet<PatternVectors> frequentPatterns = getFrequentPatterns(reports);

    DataSet<MultilevelGraphWithPatternEmbeddingsMap> grownEmbeddings = iterative
      .map(new GrowFrequentPatterns(gSpan))
      .withBroadcastSet(frequentPatterns, FSMConstants.FREQUENT_PATTERNS)
      .filter(new NotObsolete());

    // ITERATION FOOTER

    return iterative
      .closeWith(grownEmbeddings, frequentPatterns)
      // keep only collector and expand embedding map keys
      .filter(new IsFrequentPatternCollector())
      .flatMap(new ExpandFrequentPatterns());
  }

  /**
   * Triggers the postprocessing.
   *
   * @param encodedOutput frequent patterns represented by multiplexed int-arrays
   * @return Gradoop graph transactions
   */
  private DataSet<GraphTransaction> postProcess(DataSet<MultilevelGraph> encodedOutput) {
    return encodedOutput
      .map(new MultilevelPatternToEPGMGraphTransaction())
      .withBroadcastSet(vertexLabelDictionary, FSMConstants.VERTEX_DICTIONARY)
      .withBroadcastSet(edgeLabelDictionary, FSMConstants.EDGE_DICTIONARY)
      .withBroadcastSet(levelDictionary, FSMConstants.LEVEL_DICTIONARY)
      .withBroadcastSet(graphCount, FSMConstants.GRAPH_COUNT);
  }

  /**
   * Executes pruning and dictionary coding of vertex labels.
   *
   * @param graphs graphs with string-labels
   * @return graphs with dictionary-encoded vertex labels
   */
  private void createDictionaries(DataSet<GraphTransaction> graphs) {

    // LABEL PRUNING

    DataSet<Tuple3<String[], String[], String[]>> labels = graphs
      .map(new ReportLabels());


    // Determine cardinality of input graph collection
    this.graphCount = Count
      .count(labels);

    // Calculate minimum frequency
    this.minFrequency = graphCount
      .map(new MinFrequency(fsmConfig));

    DataSet<WithCount<String>> vertexLabels = labels
      .map(new Value0Of3<>())
      .flatMap(new ExpandArray<>())
      .map(new MakeCountable<>());

    vertexLabelDictionary = getFrequentLabels(vertexLabels)
      .reduceGroup(new CreateDictionary(comparator));

    DataSet<WithCount<String>> edgeLabels = labels
      .map(new Value1Of3<>())
      .flatMap(new ExpandArray<>())
      .map(new MakeCountable<>());

    edgeLabelDictionary = getFrequentLabels(edgeLabels)
      .reduceGroup(new CreateDictionary(comparator));

    DataSet<WithCount<String>> levelValues = labels
      .map(new Value2Of3<>())
      .flatMap(new ExpandArray<>())
      .map(new MakeCountable<>());

    levelDictionary = levelValues
      .groupBy(0)
      .sum(1)
      .reduceGroup(new CreateDictionary(comparator))
      .map(new InsertDummyZeroIntoDictionary());
  }

  /**
   * Determines frequent labels.
   *
   * @param labels dataset of labels
   *
   * @return dataset of frequent labels
   */
  private DataSet<WithCount<String>> getFrequentLabels(DataSet<WithCount<String>> labels) {
    // enabled
    if (fsmConfig.getDictionaryType() != DictionaryType.RANDOM) {

      labels = labels
        .groupBy(0)
        .sum(1)
        .filter(new Frequent<>())
        .withBroadcastSet(minFrequency, FSMConstants.MIN_FREQUENCY);

      // disabled
    } else {
      labels = labels
        .distinct();
    }

    return labels;
  }

  /**
   * Identifies valid frequent patterns from a dataset of reported patterns.
   *
   * @param patterns reported patterns
   * @return valid frequent patterns
   */
  private GroupReduceOperator<PatternVectors, PatternVectors> getFrequentPatterns(
    DataSet<MultilevelGraph> patterns) {

    return patterns
      .groupBy(0)
      .combineGroup(new CombineVectors(gSpan))
      .groupBy(0)
      .reduceGroup(new FrequentCrossLevelPatterns(vectorMiner))
      .withBroadcastSet(minFrequency, FSMConstants.MIN_FREQUENCY);
  }

  @Override
  public String getName() {
    return CrossLevelTFSM.class.getSimpleName();
  }
}
