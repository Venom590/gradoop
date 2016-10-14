package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.csv.pojos.Csv;
import org.gradoop.flink.io.impl.csv.pojos.Key;
import org.gradoop.flink.io.impl.csv.pojos.Label;
import org.gradoop.flink.io.impl.csv.pojos.Properties;
import org.gradoop.flink.io.impl.csv.pojos.Property;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by galpha on 9/28/16.
 */
public class CSVToVertex implements
  MapFunction<Tuple3<Csv, String, String>, ImportVertex<String>> {

  private final String datasourceName;


  private ImportVertex<String> reuse;


  public CSVToVertex(String datasourceName){
    this.datasourceName = datasourceName;
    this.reuse = new ImportVertex<>();
  }




  @Override
  public ImportVertex<String> map(Tuple3<Csv, String, String> tuple) throws
    Exception {

    Csv csv = tuple.f0;
    String domainName = tuple.f1;
    String line = tuple.f2;

    String[] fields = line.split(Pattern.quote(csv.getSeparator()));


    String key = createKey(csv.getVertex().getKey(), domainName, fields);
    String label = createLabel(csv.getVertex().getLabel(), fields);
    PropertyList properties = createProperties(csv, fields);


    reuse.setId(key);
    reuse.setLabel(label);
    reuse.setProperties(properties);



    return reuse;
  }

  private String createLabel(Label label, String[] fields) {

    String labelSeparator = label.getSeparator();
    String labelString = "";

    if (label.getRef() != null){

    }

    if (label.getReference() != null){

    }

    if (label.getRefOrReference() != null){

    }

    if (label.getStatic() != null){
      labelString = label.getStatic().getName();
    }

    return labelString;
  }

  private String createKey (Key key, String domainName,  String[] fields){

    String id = datasourceName + "_" + domainName + "_" + key.getClazz() + "_"
      + fields[0];

    return id;
  }

  private PropertyList createProperties (Csv csv, String[] fields){
    PropertyList list = PropertyList.create();



    for (Property p: csv.getVertex().getProperties().get(0).getProperty()) {
      org.gradoop.common.model.impl.properties.Property prop =
        new org.gradoop.common.model.impl.properties.Property();

      prop.setKey(p.getName());

      PropertyValue value = new PropertyValue();

      String type = csv.getColumns().getColumn().get(p.getColumnId()).getType
        ().value();

      if (type.equals("String")){
        value.setString(fields[p.getColumnId()]);
      } else if (type.equals("Integer")) {
        value.setInt(Integer.parseInt(fields[p.getColumnId()]));
      }


      prop.setValue(value);
      list.set(prop);
    }

    return list;
  }

}