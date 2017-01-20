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

package org.gradoop.flink.io.impl.csv;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.csv.functions.ElementCsvExtensionToCSVString;
import org.gradoop.flink.io.impl.csv.functions.ElementToElementCSVExtension;
import org.gradoop.flink.io.impl.csv.parser.XmlMetaParser;
import org.gradoop.flink.io.impl.csv.pojos.CsvExtension;
import org.gradoop.flink.io.impl.csv.pojos.Datasource;
import org.gradoop.flink.io.impl.csv.pojos.Domain;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.List;

/**
 * Writes an EPGM instance into CSV files. Their format has to be defined
 * with a xml file. The schema for the xml is located at
 * 'resources/dara/csv/csv_format.xsd'.
 */
public class CSVDataSink extends CSVBase implements DataSink {

  /**
   *  Creates a new data sink. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param metaXmlPath xml file
   * @param csvDir csv directory
   * @param config Gradoop Flink configuration
   */
  public CSVDataSink(String metaXmlPath, String csvDir, GradoopFlinkConfig config) {
    super(metaXmlPath, csvDir, config);
  }

  @Override
  public void write(LogicalGraph logicalGraph) throws IOException {
    write(GraphCollection.fromGraph(logicalGraph));
  }

  @Override
  public void write(GraphCollection graphCollection) throws IOException {
    DataSet<GraphHead> graphHeads = graphCollection.getGraphHeads();
    DataSet<Vertex> vertices = graphCollection.getVertices();
    DataSet<Edge> edges = graphCollection.getEdges();

  // parse the xml file to a datasource and select each csv object
    Datasource datasource = null;
    try {
      datasource = XmlMetaParser.parse(getXsdPath(), getMetaXmlPath());
    } catch (SAXException | JAXBException e) {
      e.printStackTrace();
    }
    List<Tuple2<CsvExtension, String>> graphHeadList = Lists.newArrayList();
    List<Tuple2<CsvExtension, String>> vertexList = Lists.newArrayList();
    List<Tuple2<CsvExtension, String>> edgeList = Lists.newArrayList();
    if (datasource != null) {
      for (Domain domain : datasource.getDomain()) {
        for (CsvExtension csv : domain.getCsv()) {
          csv.setDomainName(domain.getName());
          csv.setDatasourceName(datasource.getName());
          if (csv.getGraphhead() != null) {
            graphHeadList.add(new Tuple2<CsvExtension, String>(csv,
              csv.getGraphhead().getKey().getClazz()));
          }
          if (csv.getVertex() != null) {
            vertexList.add(new Tuple2<CsvExtension, String>(csv,
              csv.getVertex().getKey().getClazz()));
          }
          if (csv.getEdge() != null) {
            edgeList.add(new Tuple2<CsvExtension, String>(csv, csv.getEdge().getKey().getClazz()));
          }
        }
      }
    }

    DataSet<Tuple2<GraphHead, CsvExtension>> headTuple = graphHeads
      .map(new ElementToElementCSVExtension<GraphHead>(graphHeadList));
//    try {
//      headTuple.print();
//    } catch (Exception e) {
//      e.printStackTrace();
//    }

    DataSet<String> headCsv = headTuple
      .map(new ElementCsvExtensionToCSVString<GraphHead>());

    headCsv.writeAsText(getCsvDir() + "/graphheads.csv");

//    DataSet<Tuple2<Vertex, CsvExtension>> vertexTuple = vertices
//      .map(new ElementToElementCSVExtension<Vertex>(vertexList));
//
//    DataSet<Tuple2<Edge, CsvExtension>> edgeTuple = edges
//      .map(new ElementToElementCSVExtension<Edge>(edgeList));

  }

  @Override
  public void write(GraphTransactions graphTransactions) throws IOException {
    write(GraphCollection.fromTransactions(graphTransactions));
  }
}
