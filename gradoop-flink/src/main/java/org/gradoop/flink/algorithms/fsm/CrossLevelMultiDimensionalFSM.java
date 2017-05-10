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
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConstants;
import org.gradoop.flink.algorithms.fsm.common.config.DictionaryType;
import org.gradoop.flink.algorithms.fsm.common.comparison.AlphabeticalLabelComparator;
import org.gradoop.flink.algorithms.fsm.common.comparison.InverseProportionalLabelComparator;
import org.gradoop.flink.algorithms.fsm.common.comparison.LabelComparator;
import org.gradoop.flink.algorithms.fsm.common.comparison.ProportionalLabelComparator;
import org.gradoop.flink.algorithms.fsm.xmd.config.XMDConfig;
import org.gradoop.flink.algorithms.fsm.xmd.functions.conversion.DFSCodeToEPGMGraphTransaction;
import org.gradoop.flink.algorithms.fsm.xmd.functions.conversion.EPGMGraphTransactionToMultilevelGraph;
import org.gradoop.flink.algorithms.fsm.xmd.functions.mining.CreateCollector;
import org.gradoop.flink.algorithms.fsm.xmd.functions.mining.ExpandFrequentPatterns;
import org.gradoop.flink.algorithms.fsm.xmd.functions.mining.Frequent;
import org.gradoop.flink.algorithms.fsm.xmd.functions.mining.GrowFrequentPatterns;
import org.gradoop.flink.algorithms.fsm.xmd.functions.mining.InitSingleEdgePatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.xmd.functions.mining.IsFrequentPatternCollector;
import org.gradoop.flink.algorithms.fsm.xmd.functions.mining.NotObsolete;
import org.gradoop.flink.algorithms.fsm.xmd.functions.mining.ReportSupportedPatterns;
import org.gradoop.flink.algorithms.fsm.xmd.functions.mining.VerifyPattern;
import org.gradoop.flink.algorithms.fsm.xmd.functions.preprocessing.CreateDictionary;
import org.gradoop.flink.algorithms.fsm.xmd.functions.preprocessing.Encode;
import org.gradoop.flink.algorithms.fsm.xmd.functions.preprocessing.MinFrequency;
import org.gradoop.flink.algorithms.fsm.xmd.functions.preprocessing.NotEmpty;
import org.gradoop.flink.algorithms.fsm.xmd.functions.preprocessing.ReportLabels;
import org.gradoop.flink.algorithms.fsm.common.gspan.DirectedGSpanLogic;
import org.gradoop.flink.algorithms.fsm.common.gspan.GSpanLogic;
import org.gradoop.flink.algorithms.fsm.common.gspan.UndirectedGSpanLogic;
import org.gradoop.flink.algorithms.fsm.xmd.tuples.EncodedMultilevelGraph;
import org.gradoop.flink.algorithms.fsm.xmd.tuples.MDGraphWithPatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.xmd.tuples.MultilevelGraph;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.operators.count.Count;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.GraphTransaction;

/**
 * Gradoop operator wrapping the DIMSpan algorithm for transactional frequent subgraph mining.
 */
public class CrossLevelMultiDimensionalFSM implements UnaryCollectionToCollectionOperator {

  /**
   * Maximum number of iterations if set of k-edge frequent patterns is not running empty before.
   */
  private static final int MAX_ITERATIONS = 100;

  /**
   * FSM configuration
   */
  protected final XMDConfig fsmConfig;

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

  /**
   * Vertex label dictionary for dictionary coding.
   */
  private DataSet<String[]> labelDictionary;

  /**
   * Label comparator used for dictionary coding.
   */
  private final LabelComparator comparator;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public CrossLevelMultiDimensionalFSM(XMDConfig fsmConfig) {
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
  }

  /**
   * Constructor.
   *
   * @param minSupport minimum support threshold
   */
  public CrossLevelMultiDimensionalFSM(float minSupport) {
    this(new XMDConfig(minSupport, true));
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {

    // convert Gradoop graph collection to DIMSpan input format
    DataSet<MultilevelGraph> input = collection
      .toTransactions()
      .getTransactions()
      .map(new EPGMGraphTransactionToMultilevelGraph(fsmConfig));

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
  public DataSet<GraphTransaction> execute(DataSet<MultilevelGraph> input) {

    DataSet<EncodedMultilevelGraph> encodedInput = preProcess(input);
    DataSet<WithCount<int[]>> encodedOutput = mine(encodedInput);

    return postProcess(encodedOutput);
  }

  /**
   * Triggers the label-frequency base preprocessing
   *
   * @param graphs input
   * @return preprocessed input
   */
  private DataSet<EncodedMultilevelGraph> preProcess(DataSet<MultilevelGraph> graphs) {

    // Determine cardinality of input graph collection
    this.graphCount = Count
      .count(graphs);

    // Calculate minimum frequency
    this.minFrequency = graphCount
      .map(new MinFrequency(fsmConfig));

    // Execute vertex label pruning and dictionary coding
    createDictionary(graphs);

    // Execute edge label pruning and dictionary coding
    DataSet<EncodedMultilevelGraph> encodedGraphs = graphs
      .map(new Encode(fsmConfig))
      .withBroadcastSet(labelDictionary, FSMConstants.LABEL_DICTIONARY);

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
  protected DataSet<WithCount<int[]>> mine(DataSet<EncodedMultilevelGraph> graphs) {

    DataSet<MDGraphWithPatternEmbeddingsMap> searchSpace = graphs
      .map(new InitSingleEdgePatternEmbeddingsMap(gSpan));

    // Workaround to support multiple data sinks: create pseudo-graph (collector),
    // which embedding map will be used to union all k-edge frequent patterns
    DataSet<MDGraphWithPatternEmbeddingsMap> collector = graphs
      .getExecutionEnvironment()
      .fromElements(true)
      .map(new CreateCollector());

    searchSpace = searchSpace.union(collector);

    // ITERATION HEAD

    IterativeDataSet<MDGraphWithPatternEmbeddingsMap> iterative = searchSpace
      .iterate(MAX_ITERATIONS);

    // ITERATION BODY

    DataSet<WithCount<int[]>> reports = iterative
      .flatMap(new ReportSupportedPatterns());

    DataSet<WithCount<int[]>> frequentPatterns = getFrequentPatterns(reports);

    DataSet<MDGraphWithPatternEmbeddingsMap> grownEmbeddings = iterative
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
  private DataSet<GraphTransaction> postProcess(DataSet<WithCount<int[]>> encodedOutput) {
    return encodedOutput
      .map(new DFSCodeToEPGMGraphTransaction())
      .withBroadcastSet(labelDictionary, FSMConstants.LABEL_DICTIONARY)
      .withBroadcastSet(graphCount, FSMConstants.GRAPH_COUNT);
  }

  /**
   * Executes pruning and dictionary coding of vertex labels.
   *
   * @param graphs graphs with string-labels
   * @return graphs with dictionary-encoded vertex labels
   */
  private void createDictionary(DataSet<MultilevelGraph> graphs) {

    // LABEL PRUNING

    DataSet<WithCount<String>> labels = graphs
      .flatMap(new ReportLabels());

    labelDictionary = getFrequentLabels(labels)
      .reduceGroup(new CreateDictionary(comparator));

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
  private DataSet<WithCount<int[]>> getFrequentPatterns(DataSet<WithCount<int[]>> patterns) {
    return patterns
      .groupBy(0)
      .sum(1)
      .filter(new Frequent<>())
      .withBroadcastSet(minFrequency, FSMConstants.MIN_FREQUENCY)
      .filter(new VerifyPattern(gSpan));
  }

  @Override
  public String getName() {
    return CrossLevelMultiDimensionalFSM.class.getSimpleName();
  }
}
