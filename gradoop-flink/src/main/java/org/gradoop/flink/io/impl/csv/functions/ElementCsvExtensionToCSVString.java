package org.gradoop.flink.io.impl.csv.functions;


import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.pojos.*;

import java.io.Serializable;
import java.util.List;

public class ElementCsvExtensionToCSVString<T extends Element>
  implements MapFunction<Tuple2<T, CsvExtension>, String> {

  @Override
  public String map(Tuple2<T, CsvExtension> tuple) throws Exception {
    T element = tuple.f0;
    CsvExtension csvExtension = tuple.f1;
    StringBuilder csvString = new StringBuilder();

    List<Column> columns = csvExtension.getColumns().getColumn();
    //the result line
    List<String> line = Lists.newArrayList();


    Key key = null;
    Label label = null;
    List<Property> properties = null;
    List<Graph> graphs = null;
    Target target = null;
    Source source = null;

    //get all meta information from csvExtension
    if (csvExtension.getGraphhead() != null) {
      key = csvExtension.getGraphhead().getKey();
      label = csvExtension.getGraphhead().getLabel();
      properties = csvExtension.getGraphhead().getProperties().getProperty();
    }
    if (csvExtension.getVertex() != null) {
      key = csvExtension.getVertex().getKey();
      label = csvExtension.getVertex().getLabel();
      properties = csvExtension.getVertex().getProperties().getProperty();
      graphs = csvExtension.getVertex().getGraphs().getGraph();
      //if vertex has a vertex edge
      if (csvExtension.getVertex().getEdges() != null) {
        List<Vertexedge> vertexEdges = csvExtension.getVertex().getEdges().getVertexedge();
        for (Vertexedge vertexEdge : vertexEdges) {
          target = vertexEdge.getTarget();
        }
      }
    }
    if (csvExtension.getEdge() != null) {
      key = csvExtension.getEdge().getKey();
      label = csvExtension.getEdge().getLabel();
      properties = csvExtension.getEdge().getProperties().getProperty();
      graphs = csvExtension.getEdge().getGraphs().getGraph();
      target = csvExtension.getEdge().getTarget();
      source = csvExtension.getEdge().getSource();
    }

    ///////////////////////// KEY ///////////////////////////////

    //get all column ids, where the ids are taken from,
    List<Integer> keyColumns = Lists.newArrayList();
    for (Serializable object : key.getContent()) {
      //only ref is important
      if (Ref.class.isInstance(object)) {
        keyColumns.add(((Ref) object).getColumnId().intValue());
      //otherwise a static is used -1 will be stored to keep the right order
      } else {
        keyColumns.add(-1);
      }
    }

    //complete key stored as property in the element
    String fullKey = element.getPropertyValue("key").getString();
    //only the id part
    String idKey = fullKey.split(CSVConstants.SEPARATOR_ID_START)[1];
    //all single ids
    String[] ids = idKey.split(CSVConstants.SEPARATOR_ID);


    //add all ids to their related position
    for (int i = 0; i < ids.length; i++) {
      if (keyColumns.get(i) >= 0){
        line.add(keyColumns.get(i), ids[i]);
      }
    }

    /////////////////////// LABEL /////////////////////////////////

    List<Integer> labelColumns = Lists.newArrayList();
    for (Serializable object : label.getContent()) {
      //only ref is important
      if (Ref.class.isInstance(object)) {
        labelColumns.add(((Ref) object).getColumnId().intValue());
        //otherwise a static is used -1 will be stored to keep the right order
      } else {
        labelColumns.add(-1);
      }
    }

    //complete label
    String fullLabel = element.getLabel();
    //get the label separator if specified or take default one
    String labelSeparator;
    if (label.getSeparator() != null && !label.getSeparator().equals("")) {
      labelSeparator = label.getSeparator();
    } else {
      labelSeparator = CSVConstants.SEPARATOR_LABEL;
    }
    //split label by separator
    String[] labels = fullLabel.split(labelSeparator);

    //add all labels to their related position
    for (int i = 0; i < labels.length; i++) {
      if (labelColumns.get(i) >= 0){
        line.add(labelColumns.get(i), labels[i]);
      }
    }


    ////////////////////// PROPERTIES ////////////////////////////

    for (Property property : properties) {
      line.add(property.getColumnId(), element.getPropertyValue(property.getName()).getString());
    }

    /////////////////////////// GRAPHS /////////////////////////////

    List<Integer> graphColumns = Lists.newArrayList();
    if (graphs != null) {
      //take all graph entries
      for (Graph graph : graphs) {
        //take their content static, ref or reference
        for (Serializable object : graph.getStaticOrRefOrReference()) {
          //if ref then add column id
          if (Ref.class.isInstance(object)) {
            graphColumns.add(((Ref) object).getColumnId().intValue());
          //if reference
          } else if (Reference.class.isInstance(object)) {
            //take its key
            for (Serializable serializable : ((Reference) object).getKey().getContent()) {
              //if key has ref take the column id
              if (Ref.class.isInstance(serializable)) {
                graphColumns.add(((Ref) serializable).getColumnId().intValue());
                //otherwise a static is used -1 will be stored to keep the right order
              } else {
                graphColumns.add(-1);
              }
            }
          //otherwise a static is used -1 will be stored to keep the right order
          } else {
            graphColumns.add(-1);
          }
        }
      }
    }

    //////////////////////// SOURCE AND TARGET /////////////////////////////////////


//    if (source != null && target != null) {
//      org.gradoop.common.model.impl.pojo.Edge edge =
//        (org.gradoop.common.model.impl.pojo.Edge) element;
//
//      source.
//    }





    //take all entries from the line and append them with separator
    StringBuilder result = new StringBuilder();
    for (String entry : line) {
      result.append(entry.replace(csvExtension.getSeparator(), CSVConstants.EDCAPE_SEPARATOR_LINE));
      result.append(csvExtension.getSeparator());
    }

    return result.toString();
  }
}
