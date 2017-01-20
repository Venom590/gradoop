package org.gradoop.flink.io.impl.csv;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GraphCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Collection;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;

public class CSVDataSinkTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWrite() throws Exception {

    //READ
    // paths to input files
    String csvFiles = CSVDataSourceTest.class.getResource("/data/csv/").getPath();
    String metaXmlFile = CSVDataSourceTest.class.getResource("/data/csv/test1.xml").getFile();
    // create datasource
    DataSource dataSource = new CSVDataSource(metaXmlFile, csvFiles,config);
    // get collection
    GraphCollection collection = dataSource.getGraphCollection();
    //expected collections
    Collection<GraphHead> expectedGraphHeads = Lists.newArrayList();
    Collection<Vertex> expectedVertices = Lists.newArrayList();
    Collection<Edge> expectedEdges = Lists.newArrayList();
    collection.getGraphHeads()
            .output(new LocalCollectionOutputFormat<>(expectedGraphHeads));
    collection.getVertices()
            .output(new LocalCollectionOutputFormat<>(expectedVertices));
    collection.getEdges()
            .output(new LocalCollectionOutputFormat<>(expectedEdges));

    //WRITE
    String tmpDir = temporaryFolder.getRoot().toString();
    csvFiles = tmpDir + "/csv/";
    collection.writeTo(new CSVDataSink(metaXmlFile, csvFiles, config));

    DataSource writtenDataSource = new CSVDataSource(metaXmlFile, csvFiles,config);
    GraphCollection writtenCollection = writtenDataSource.getGraphCollection();

    Collection<GraphHead> loadedGraphHeads    = Lists.newArrayList();
    Collection<Vertex>    loadedVertices      = Lists.newArrayList();
    Collection<Edge>      loadedEdges         = Lists.newArrayList();
    writtenCollection.getGraphHeads()
      .output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    writtenCollection.getVertices()
      .output(new LocalCollectionOutputFormat<>(loadedVertices));
    writtenCollection.getEdges()
      .output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(expectedGraphHeads, loadedGraphHeads);
    validateEPGMElementCollections(expectedVertices, loadedVertices);
    validateEPGMGraphElementCollections(expectedVertices, loadedVertices);
    validateEPGMElementCollections(expectedEdges, loadedEdges);
    validateEPGMGraphElementCollections(expectedEdges, loadedEdges);
  }
}
