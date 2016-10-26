package org.gradoop.flink.io.impl.csv.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.csv.pojos.*;
import org.gradoop.flink.io.impl.csv.tuples.ReferenceTuple;

import java.util.List;
import java.util.regex.Pattern;


public class CSVToElement implements
  FlatMapFunction<Tuple2<Csv, List<String>>, EPGMElement> {

  public static final String SEPARATOR_KEY = "_";

  public static final String ESCAPE_REPLACEMENT_KEY = "&lowbar;";

  public static final String SEPARATOR_GRAPHS = "%";

  public static final String ESCAPE_REPLACEMENT_GRAPHS = "&percnt;";

  public static final String SEPARATOR_LABEL = ";";

  public static final String ESCAPE_REPLACEMENT_LABEL = "&semi;";

  public static final String PROPERTY_KEY_SOURCE = "source";

  public static final String PROPERTY_KEY_TARGET = "target";

  public static final String PROPERTY_KEY_GRAPHS = "graphs";

  public static final String PROPERTY_KEY_KEY = "key";

  private EPGMElement reuse;

  private GraphHeadFactory graphHeadFactory;
  private VertexFactory vertexFactory;
  private EdgeFactory edgeFactory;


  public CSVToElement(GraphHeadFactory graphHeadFactory,
    VertexFactory vertexFactory, EdgeFactory edgeFactory) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
  }

  @Override
  public void flatMap(Tuple2<Csv, List<String>> tuple,
    Collector<EPGMElement> collector) throws Exception {

    Csv csv = tuple.f0;
    List<String> content = tuple.f1;

    boolean isEdge = false;
    boolean isVertexEdge;



    for (String line : content) {

      String[] fields = line.split(Pattern.quote(csv.getSeparator()));

      String label = "";
      Key key = null;
      List<Properties> propertiesCsv = null;

      String datasourceName = csv.getDatasourceName();
      String domainName = csv.getDomainName();
      String className = "";

      isVertexEdge = false;

      List<Graph> graphs = Lists.newArrayList();

      if (csv.getGraphhead() != null) {
//        reuse = graphHeadFactory.createGraphHead();
//        if (csv.getGraphhead().getLabel() != null) {
//          label = createLabel(csv.getGraphhead().getLabel(), fields);
//        }
//        key = csv.getGraphhead().getKey();
//        propertiesCsv = csv.getGraphhead().getProperties();
//        className = csv.getGraphhead().getKey().getClazz();

        reuse = createGraphHead(csv, fields);

      } else if (csv.getVertex() != null) {
        reuse = vertexFactory.createVertex();
        if (csv.getVertex().getLabel() != null) {
          label = createLabel(csv.getVertex().getLabel(), fields);
        }
        key = csv.getVertex().getKey();
        propertiesCsv = csv.getVertex().getProperties();
        className = csv.getVertex().getKey().getClazz();
        graphs = csv.getVertex().getGraphs().getGraph();
        if (csv.getVertex().getEdges() != null) {
          isVertexEdge = true;
        }

      } else if (csv.getEdge() != null) {
//        reuse = edgeFactory.createEdge(GradoopId.get(), GradoopId.get());
//        isEdge = true;
//        if (csv.getEdge().getLabel() != null) {
//          label = createLabel(csv.getEdge().getLabel(), fields);
//        }
//        key = csv.getEdge().getKey();
//        propertiesCsv = csv.getEdge().getProperties();
//        className = csv.getEdge().getKey().getClazz();
//        graphs = csv.getEdge().getGraphs().getGraph();

        reuse = createEdge(csv, fields);

      } else {
        reuse = null;
      }

      StringBuilder sb = new StringBuilder();
      boolean notFirst = false;
      for (Graph graph : graphs) {
        if (!notFirst) {
          notFirst = true;
        } else {
          sb.append(SEPARATOR_GRAPHS);
        }
        sb.append(createKey(this.setNamesAndIds(
          graph, fields, datasourceName, domainName, className))
          .replaceAll(SEPARATOR_GRAPHS, ESCAPE_REPLACEMENT_GRAPHS));
      }

      PropertyList properties =
        createProperties(csv, propertiesCsv, key, fields);



      reuse.setId(GradoopId.get());
      reuse.setLabel(label);
      reuse.setProperties(properties);
      reuse.getProperties().set(PROPERTY_KEY_GRAPHS, sb.toString());


      String sourceKey;
      String targetKey;
      ReferenceTuple referenceTuple;
      if (isEdge) {
        referenceTuple = this
          .setNamesAndIds(csv.getEdge().getSource(), fields, datasourceName,
            domainName, className);

        sourceKey = createKey(referenceTuple);
        referenceTuple = this
          .setNamesAndIds(csv.getEdge().getTarget(), fields, datasourceName,
            domainName, className);

        targetKey = createKey(referenceTuple);

        reuse.getProperties().set(PROPERTY_KEY_SOURCE, sourceKey);
        reuse.getProperties().set(PROPERTY_KEY_TARGET, targetKey);

      }
      if (isVertexEdge) {
        org.gradoop.common.model.impl.pojo.Edge edge = edgeFactory.createEdge(
            GradoopId.get(), GradoopId.get());
        for (Vertexedge vertexEdge : csv.getVertex().getEdges().getVertexedge())
          {
          className = vertexEdge.getKey().getClazz();
          if (vertexEdge.getLabel() != null) {
            label = createLabel(vertexEdge.getLabel(), fields);
          }
          key = vertexEdge.getKey();
          propertiesCsv = vertexEdge.getProperties();
          graphs = vertexEdge.getGraphs().getGraph();

          notFirst = false;
          for (Graph graph : graphs) {
            if (!notFirst) {
              notFirst = true;
            } else {
              sb.append(SEPARATOR_GRAPHS);
            }
            sb.append(createKey(this.setNamesAndIds(
              graph, fields, datasourceName, domainName, className))
              .replaceAll(SEPARATOR_GRAPHS, ESCAPE_REPLACEMENT_GRAPHS));
          }

          properties =
            createProperties(csv, propertiesCsv, key, fields);

          edge.setId(GradoopId.get());
          edge.setLabel(label);
          edge.setProperties(properties);
          edge.getProperties().set(PROPERTY_KEY_GRAPHS, sb.toString());


          referenceTuple = this.setNamesAndIds(
            vertexEdge.getTarget(), fields, datasourceName, domainName, className);

          targetKey = createKey(referenceTuple);

          reuse.getProperties().set(PROPERTY_KEY_SOURCE, reuse
            .getPropertyValue(PROPERTY_KEY_KEY));
          reuse.getProperties().set(PROPERTY_KEY_TARGET, targetKey);

          collector.collect(edge);
        }
      }
      collector.collect(reuse);
    }
  }

  private org.gradoop.common.model.impl.pojo.GraphHead createGraphHead(Csv csv,
  String[] fields) {
    String label = "";
    if (csv.getGraphhead().getLabel() != null) {
      label = createLabel(csv.getGraphhead().getLabel(), fields);
    }
    Key key = csv.getGraphhead().getKey();
    List<Properties> propertiesCsv = csv.getGraphhead().getProperties();
    PropertyList properties =
      createProperties(csv, propertiesCsv, key, fields);

    return graphHeadFactory.createGraphHead(label, properties);
  }




  private void createVertex(Csv csv, String[] fields) {
    String label = "";
    boolean isVertexEdge = false;
    reuse = vertexFactory.createVertex();
    if (csv.getVertex().getLabel() != null) {
      label = createLabel(csv.getVertex().getLabel(), fields);
    }
    Key key = csv.getVertex().getKey();
    List<Properties> propertiesCsv = csv.getVertex().getProperties();
    String className = csv.getVertex().getKey().getClazz();
    List<Graph> graphs = csv.getVertex().getGraphs().getGraph();
    if (csv.getVertex().getEdges() != null) {
      isVertexEdge = true;
    }

    StringBuilder sb = new StringBuilder();
    boolean notFirst = false;
    for (Graph graph : graphs) {
      if (!notFirst) {
        notFirst = true;
      } else {
        sb.append(SEPARATOR_GRAPHS);
      }
      sb.append(createKey(this.setNamesAndIds(
        graph, fields, csv.getDatasourceName(), csv.getDomainName(), className))
        .replaceAll(SEPARATOR_GRAPHS, ESCAPE_REPLACEMENT_GRAPHS));
    }

    PropertyList properties =
      createProperties(csv, propertiesCsv, key, fields);



    reuse.setId(GradoopId.get());
    reuse.setLabel(label);
    reuse.setProperties(properties);
    reuse.getProperties().set(PROPERTY_KEY_GRAPHS, sb.toString());




    if (isVertexEdge) {
      org.gradoop.common.model.impl.pojo.Edge edge = edgeFactory.createEdge(
        GradoopId.get(), GradoopId.get());
      for (Vertexedge vertexEdge : csv.getVertex().getEdges().getVertexedge()) {
        className = vertexEdge.getKey().getClazz();
        if (vertexEdge.getLabel() != null) {
          label = createLabel(vertexEdge.getLabel(), fields);
        }
        key = vertexEdge.getKey();
        propertiesCsv = vertexEdge.getProperties();
        graphs = vertexEdge.getGraphs().getGraph();

        notFirst = false;
        for (Graph graph : graphs) {
          if (!notFirst) {
            notFirst = true;
          } else {
            sb.append(SEPARATOR_GRAPHS);
          }
          sb.append(createKey(this
            .setNamesAndIds(graph, fields, datasourceName, domainName, className))
            .replaceAll(SEPARATOR_GRAPHS, ESCAPE_REPLACEMENT_GRAPHS));
        }

        properties = createProperties(csv, propertiesCsv, key, fields);

        edge.setId(GradoopId.get());
        edge.setLabel(label);
        edge.setProperties(properties);
        edge.getProperties().set(PROPERTY_KEY_GRAPHS, sb.toString());


        referenceTuple = this
          .setNamesAndIds(vertexEdge.getTarget(), fields, datasourceName,
            domainName, className);

        targetKey = createKey(referenceTuple);

        reuse.getProperties().set(PROPERTY_KEY_SOURCE, reuse.getPropertyValue(PROPERTY_KEY_KEY));
        reuse.getProperties().set(PROPERTY_KEY_TARGET, targetKey);

        collector.collect(edge);
      }
    }
  }


  private org.gradoop.common.model.impl.pojo.Edge createEdge(Csv csv,
    String[] fields) {
    String label = "";

    if (csv.getEdge().getLabel() != null) {
      label = createLabel(csv.getEdge().getLabel(), fields);
    }
    Key key = csv.getEdge().getKey();
    List<Properties> propertiesCsv = csv.getEdge().getProperties();
    String className = csv.getEdge().getKey().getClazz();
    List<Graph> graphs = csv.getEdge().getGraphs().getGraph();


    String graphList = createGraphList(graphs, csv.getDatasourceName(), csv
      .getDomainName(), className, fields);

    PropertyList properties =
      createProperties(csv, propertiesCsv, key, fields);
    properties.set(PROPERTY_KEY_GRAPHS, graphList);


    ReferenceTuple referenceTuple;

    referenceTuple = this.setNamesAndIds(
      csv.getEdge().getSource(), fields, csv.getDatasourceName(),
        csv.getDomainName(), className);

    String sourceKey = createKey(referenceTuple);
    referenceTuple = this.setNamesAndIds(
      csv.getEdge().getTarget(), fields, csv.getDatasourceName(),
        csv.getDomainName(), className);

    String targetKey = createKey(referenceTuple);

    properties.set(PROPERTY_KEY_SOURCE, sourceKey);
    properties.set(PROPERTY_KEY_TARGET, targetKey);

    return edgeFactory.createEdge(label, GradoopId.get(), GradoopId.get(), properties);
  }

  private String createGraphList(List<Graph> graphs, String datasourceName,
    String domainName, String className, String[] fields) {
    StringBuilder sb = new StringBuilder();
    boolean notFirst = false;
    for (Graph graph : graphs) {
      if (!notFirst) {
        notFirst = true;
      } else {
        sb.append(SEPARATOR_GRAPHS);
      }
      sb.append(createKey(this.setNamesAndIds(
        graph, fields, datasourceName, domainName, className))
        .replaceAll(SEPARATOR_GRAPHS, ESCAPE_REPLACEMENT_GRAPHS));
    }
    return sb.toString();
  }

  private ReferenceTuple setNamesAndIds(Staticorreference staticOrReference,
    String[] fields, String datasourceName, String domainName,
    String className) {
    ReferenceTuple tuple = new ReferenceTuple();
    tuple.setDatasourceName(datasourceName);
    tuple.setDomainName(domainName);
    tuple.setClassName(className);
    tuple.setId((staticOrReference.getStatic() != null) ?
      staticOrReference.getStatic().getName() : "");

    boolean refSet = false;
    for (Ref ref : staticOrReference.getRef()) {
      tuple.setId(tuple.getId() + fields[ref.getColumnId().intValue()]);
      refSet = true;
    }

    for (Reference reference : staticOrReference.getReference()) {
      if (reference.getDatasourceName() != null) {
        tuple.setDatasourceName(reference.getDatasourceName());
      }
      if (reference.getDomainName() != null) {
        tuple.setDomainName(reference.getDomainName());
      }
      if (refSet) {
        tuple
          .setClassName(tuple.getClassName() + reference.getKey().getClazz());
      } else {
        tuple.setClassName(reference.getKey().getClazz());
      }
      tuple
        .setId(tuple.getId() + this.getEntriesFromStaticOrRef(reference
          .getKey().getContent(), fields, ""));
    }

    return tuple;
  }

  private ReferenceTuple setNamesAndIds(Objectreferences objectReferences,
    String[] fields, String datasourceName, String domainName,
    String className) {
    ReferenceTuple tuple = new ReferenceTuple();
    tuple.setDatasourceName(datasourceName);
    tuple.setDomainName(domainName);
    tuple.setClassName(className);
    tuple.setId("");

    boolean refSet = false;
    for (Object object : objectReferences.getStaticOrRefOrReference()) {
      if (Static.class.isInstance(object)) {
        tuple.setId(((Static)object).getName());
      } else  if (Ref.class.isInstance(object)) {
        tuple.setId(
          tuple.getId() + fields[((Ref)object).getColumnId().intValue()]);
        refSet = true;
      } else if (Reference.class.isInstance(object)) {
        Reference reference = (Reference) object;
        if (reference.getDatasourceName() != null) {
          tuple.setDatasourceName(reference.getDatasourceName());
        }
        if (reference.getDomainName() != null) {
          tuple.setDomainName(reference.getDomainName());
        }
        if (refSet) {
          tuple
            .setClassName(tuple.getClassName() + reference.getKey().getClazz());
        } else {
          tuple.setClassName(reference.getKey().getClazz());
        }
        tuple
          .setId(tuple.getId() + this.getEntriesFromStaticOrRef(reference
            .getKey().getContent(), fields, ""));
      }
    }
    return tuple;
  }

  private String getEntriesFromStaticOrRef(List<Object> objects, String[]
    fields, String separator) {
    String contentString = "";
    for (Object object : objects) {
      if (Static.class.isInstance(object)) {
        contentString = ((Static) object).getName();
      } else if (Ref.class.isInstance(object)) {
        if (!contentString.equals("") && !separator.equals("")) {
          contentString += separator;
        }
        if (separator.equals(SEPARATOR_LABEL)) {
          contentString += fields[((Ref) object).getColumnId().intValue()]
            .replaceAll(SEPARATOR_LABEL, ESCAPE_REPLACEMENT_LABEL);
        } else {
          contentString += fields[((Ref) object).getColumnId().intValue()];
        }
      }
    }
    return contentString;
  }


  private String createLabel(Label label, String[] fields) {
    String labelSeparator = (label.getSeparator() == null) ?
      SEPARATOR_LABEL : label.getSeparator();
    return getEntriesFromStaticOrRef(label.getContent(),
      fields, labelSeparator);
  }

  private String createKey(ReferenceTuple tuple) {

    String newId = tuple.getDatasourceName().replaceAll(SEPARATOR_KEY,
      ESCAPE_REPLACEMENT_KEY) + "_" +
      tuple.getDomainName().replaceAll(SEPARATOR_KEY, ESCAPE_REPLACEMENT_KEY)
      + "_" +
      tuple.getClassName().replaceAll(SEPARATOR_KEY, ESCAPE_REPLACEMENT_KEY)
      + "_" +
      tuple.getId().replaceAll(SEPARATOR_KEY, ESCAPE_REPLACEMENT_KEY);

    return newId;
  }


  private PropertyList createProperties(Csv csv, List<Properties> properties,
    Key key, String[] fields) {
    PropertyList list = PropertyList.create();
    String resultKey = createKey(
      new ReferenceTuple(csv.getDatasourceName(), csv.getDomainName(),
        key.getClazz(), fields[0]));

    PropertyValue value = new PropertyValue();
    value.setString(resultKey);

    list.set(PROPERTY_KEY_KEY, value);

    if (properties != null && !properties.isEmpty()) {
      for (Property p : properties.get(0).getProperty()) {
        org.gradoop.common.model.impl.properties.Property prop = new org.gradoop.common.model.impl.properties.Property();

        prop.setKey(p.getName());

        value = new PropertyValue();

        String type =
          csv.getColumns().getColumn().get(p.getColumnId()).getType().value();

        switch (type) {
          case "String":
            value.setString(fields[p.getColumnId()]);
            break;
          case "Integer":
            value.setInt(Integer.parseInt(fields[p.getColumnId()]));
            break;
          case "Long":
            value.setLong(Long.parseLong(fields[p.getColumnId()]));
            break;
          case "Float":
            value.setFloat(Float.parseFloat(fields[p.getColumnId()]));
            break;
          case "Double":
            value.setDouble(Double.parseDouble(fields[p.getColumnId()]));
            break;
          case "Boolean":
            value.setBoolean(Boolean.parseBoolean(fields[p.getColumnId()]));
            break;
          default:
            value.setString(fields[p.getColumnId()]);
            break;
        }

        prop.setValue(value);
        list.set(prop);
      }
    }
    return list;
  }
}
