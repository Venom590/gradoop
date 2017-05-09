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

package org.gradoop.flink.datagen.transactions.foodbroker.functions.process;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Returns transactional data created in a complaint handling process together with new created
 * master data (user, clients).
 */
public class ComplaintHandling
  extends AbstractProcess
  implements FlatMapFunction<GraphTransaction, Tuple2<GraphTransaction, Set<Vertex>>> {
  /**
   * List of employees. Used to create new user.
   */
  private List<Vertex> employees;
  /**
   * List of customers. Used to create new clients.
   */
  private List<Vertex> customers;
  /**
   * Map wich stores the vertex for each gradoop id.
   */
  private Map<GradoopId, Vertex> masterDataMap;
  /**
   * Set containing all sales order lines for one graph transaction.
   */
  private Set<Edge> salesOrderLines;
  /**
   * Set containing all purch order lines for one graph transaction.
   */
  private Set<Edge> purchOrderLines;
  /**
   * The sales order from one graph transaction.
   */
  private Vertex salesOrder;

  /**
   * Valued constructor.
   *
   * @param graphHeadFactory EPGM graph head factory
   * @param vertexFactory EPGM vertex factory
   * @param edgeFactory EPGM edge factory
   * @param config FoodBroker configuration
   * @param globalSeed global seed
   */
  public ComplaintHandling(GraphHeadFactory graphHeadFactory,
    VertexFactory vertexFactory, EdgeFactory edgeFactory,
    FoodBrokerConfig config, long globalSeed) {
    super(graphHeadFactory, vertexFactory, edgeFactory, config);
    this.globalSeed = globalSeed;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    employees = getRuntimeContext().getBroadcastVariable(Constants.EMPLOYEE_VERTEX_LABEL);
    customers = getRuntimeContext().getBroadcastVariable(Constants.CUSTOMER_VERTEX_LABEL);
  }

  @Override
  public void flatMap(GraphTransaction transaction,
    Collector<Tuple2<GraphTransaction, Set<Vertex>>> collector) throws Exception {
    GraphHead graphHead;
    GraphTransaction graphTransaction;
    Set<Vertex> vertices;
    Set<Edge> edges;
    Set<Vertex> deliveryNotes;
    //init new maps
    vertexMap = Maps.newHashMap();
    masterDataMap = Maps.newHashMap();
    userMap = Maps.newHashMap();

    edgeMap = createEdgeMap(transaction);
    //get needed transactional objects created during brokerage process
    deliveryNotes = getVertexByLabel(transaction, Constants.DELIVERYNOTE_VERTEX_LABEL);
    salesOrderLines = getEdgesByLabel(transaction, Constants.SALESORDERLINE_EDGE_LABEL);
    purchOrderLines = getEdgesByLabel(transaction, Constants.PURCHORDERLINE_EDGE_LABEL);
    salesOrder = getVertexByLabel(transaction, Constants.SALESORDER_VERTEX_LABEL).iterator().next();

    //create new graph head
    graphHead = graphHeadFactory.createGraphHead();
    graphIds = new GradoopIdList();
    graphIds.add(graphHead.getId());
    graphTransaction = new GraphTransaction();
    //the complaint handling process
    badQuality(deliveryNotes);
    lateDelivery(deliveryNotes);
    //get all created vertices and edges
    vertices = getVertices();
    edges = getEdges();
    //if one or more tickets were created
    if ((vertices.size() > 0) && (edges.size() > 0)) {
      graphTransaction.setGraphHead(graphHead);
      graphTransaction.setVertices(vertices);
      graphTransaction.setEdges(edges);
      collector.collect(new Tuple2<>(graphTransaction, getMasterData()));
      globalSeed++;
    }
  }

  /**
   * Creates a ticket if bad quality occurs.
   *
   * @param deliveryNotes all deliverynotes from the brokerage process
   */
  private void badQuality(Set<Vertex> deliveryNotes) {
    GradoopId purchOrderId;
    List<Float> influencingMasterQuality;
    Set<Edge> currentPurchOrderLines;
    Set<Edge> badSalesOrderLines;

    for (Vertex deliveryNote : deliveryNotes) {
      influencingMasterQuality = Lists.newArrayList();
      badSalesOrderLines = Sets.newHashSet();
      //get the corresponding purch order and purch order lines
      purchOrderId = getEdgeTargetId(Constants.CONTAINS_EDGE_LABEL, deliveryNote.getId());
      currentPurchOrderLines = getPurchOrderLinesByPurchOrder(purchOrderId);

      for (Edge purchOrderLine : currentPurchOrderLines) {
        influencingMasterQuality.add(productQualityMap.get(purchOrderLine.getTargetId()));
      }
      int containedProducts = influencingMasterQuality.size();
      // increase relative influence of vendor and logistics
      for (int i = 1; i <= containedProducts / 2; i++) {
        influencingMasterQuality.add(getEdgeTargetQuality(
          Constants.OPERATEDBY_EDGE_LABEL, deliveryNote.getId(), Constants.LOGISTIC_MAP_BC));
        influencingMasterQuality.add(getEdgeTargetQuality(Constants.PLACEDAT_EDGE_LABEL,
          purchOrderId, Constants.VENDOR_MAP_BC));
      }
      if (config.happensTransitionConfiguration(
        influencingMasterQuality, Constants.TICKET_VERTEX_LABEL,
        Constants.TI_BADQUALITYPROBABILITY_CONFIG_KEY, false)) {

        for (Edge purchOrderLine : currentPurchOrderLines) {
          badSalesOrderLines.add(getCorrespondingSalesOrderLine(purchOrderLine.getId()));
        }

        Vertex ticket = newTicket(
          Constants.BADQUALITY_TICKET_PROBLEM,
          deliveryNote.getPropertyValue(Constants.DATE_KEY).getDate());
        grantSalesRefund(badSalesOrderLines, ticket);
        claimPurchRefund(currentPurchOrderLines, ticket);
      }
    }
  }

  /**
   * Creates a ticket if late delivery occurs.
   *
   * @param deliveryNotes all deliverynotes from the brokerage process
   */
  private void lateDelivery(Set<Vertex> deliveryNotes) {
    Set<Edge> lateSalesOrderLines = Sets.newHashSet();

    // Iterate over all delivery notes and take the sales order lines of
    // sales orders, which are late
    for (Vertex deliveryNote : deliveryNotes) {
      if (deliveryNote.getPropertyValue(Constants.DATE_KEY).getDate()
        .isAfter(salesOrder.getPropertyValue(Constants.DELIVERYDATE_KEY).getDate())) {
        lateSalesOrderLines.addAll(salesOrderLines);
      }
    }

    // If we have late sales order lines
    if (!lateSalesOrderLines.isEmpty()) {
      // Collect the respective late purch order lines
      Set<Edge> latePurchOrderLines = Sets.newHashSet();
      for (Edge salesOrderLine : lateSalesOrderLines) {
        latePurchOrderLines.add(getCorrespondingPurchOrderLine(salesOrderLine.getId()));
      }
      LocalDate createdDate = salesOrder.getPropertyValue(Constants.DELIVERYDATE_KEY)
        .getDate().plusDays(1);

      // Create ticket and process refunds
      Vertex ticket = newTicket(Constants.LATEDELIVERY_TICKET_PROBLEM, createdDate);
      grantSalesRefund(lateSalesOrderLines, ticket);
      claimPurchRefund(latePurchOrderLines, ticket);
    }
  }

  /**
   * Create the ticket itself and corresponding master data with edges.
   *
   * @param problem the reason for the ticket, either bad quality or late delivery
   * @param createdAt creation date
   * @return the ticket
   */
  private Vertex newTicket(String problem, LocalDate createdAt) {
    String label = Constants.TICKET_VERTEX_LABEL;
    Properties properties = new Properties();
    // properties
    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set(Constants.CREATEDATE_KEY, createdAt);
    properties.set(Constants.PROBLEM_KEY, problem);
    properties.set(Constants.ERPSONUM_KEY, salesOrder.getId().toString());

    GradoopId employeeId = getNextEmployee();
    GradoopId customerId = getEdgeTargetId(Constants.RECEIVEDFROM_EDGE_LABEL, salesOrder.getId());

    Vertex ticket = newVertex(label, properties);

    newEdge(Constants.CONCERNS_EDGE_LABEL, ticket.getId(), salesOrder.getId());
    //new master data, user
    Vertex user = getUserFromEmployeeId(employeeId);
    newEdge(Constants.CREATEDBY_EDGE_LABEL, ticket.getId(), user.getId());
    //new master data, user
    employeeId = getNextEmployee();
    user = getUserFromEmployeeId(employeeId);
    newEdge(Constants.ALLOCATEDTO_EDGE_LABEL, ticket.getId(), user.getId());
    //new master data, client
    Vertex client = getClientFromCustomerId(customerId);
    newEdge(Constants.OPENEDBY_EDGE_LABEL, ticket.getId(), client.getId());

    return ticket;
  }

  /**
   * Calculates the refund amount to grant and creates a sales invoice with corresponding edges.
   *
   * @param salesOrderLines sales order lines to calculate refund amount
   * @param ticket the ticket created for the refund
   */
  private void grantSalesRefund(Set<Edge> salesOrderLines, Vertex ticket) {
    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality(
      Constants.ALLOCATEDTO_EDGE_LABEL, ticket.getId(), Constants.USER_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(Constants.RECEIVEDFROM_EDGE_LABEL,
      salesOrder.getId(), Constants.CUSTOMER_MAP_BC));
    //calculate refund
    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(
        influencingMasterQuality, Constants.TICKET_VERTEX_LABEL,
        Constants.TI_SALESREFUND_CONFIG_KEY, false);
    BigDecimal refundAmount = BigDecimal.ZERO;
    BigDecimal salesAmount;

    for (Edge salesOrderLine : salesOrderLines) {
      salesAmount = BigDecimal.valueOf(
        salesOrderLine.getPropertyValue(Constants.QUANTITY_KEY).getInt())
        .multiply(salesOrderLine.getPropertyValue(Constants.SALESPRICE_KEY).getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      refundAmount = refundAmount.add(salesAmount);
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);
    //create sales invoice if refund is negative
    if (refundAmount.floatValue() < 0) {
      String label = Constants.SALESINVOICE_VERTEX_LABEL;

      Properties properties = new Properties();
      properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
      properties.set(
        Constants.DATE_KEY, ticket.getPropertyValue(Constants.CREATEDATE_KEY).getDate());
      String bid = createBusinessIdentifier(currentId++, Constants.SALESINVOICE_ACRONYM);
      properties.set(Constants.SOURCEID_KEY, Constants.CIT_ACRONYM + "_" + bid);
      properties.set(Constants.REVENUE_KEY, refundAmount);
      properties.set(Constants.TEXT_KEY, Constants.TEXT_CONTENT + ticket.getId());

      Vertex salesInvoice = newVertex(label, properties);

      newEdge(Constants.CREATEDFOR_EDGE_LABEL, salesInvoice.getId(), salesOrder.getId());
    }
  }

  /**
   * Calculates the refund amount to claim and creates a purch invoice with corresponding edges.
   *
   * @param purchOrderLines purch order lines to calculate refund amount
   * @param ticket the ticket created for the refund
   */
  private void claimPurchRefund(Set<Edge> purchOrderLines, Vertex ticket) {
    GradoopId purchOrderId = purchOrderLines.iterator().next().getSourceId();

    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality(
      Constants.ALLOCATEDTO_EDGE_LABEL, ticket.getId(), Constants.USER_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality(Constants.PLACEDAT_EDGE_LABEL,
      purchOrderId, Constants.VENDOR_MAP_BC));
    //calculate refund
    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(
        influencingMasterQuality, Constants.TICKET_VERTEX_LABEL,
        Constants.TI_PURCHREFUND_CONFIG_KEY, true);
    BigDecimal refundAmount = BigDecimal.ZERO;
    BigDecimal purchAmount;

    for (Edge purchOrderLine : purchOrderLines) {
      purchAmount = BigDecimal.valueOf(
        purchOrderLine.getPropertyValue(Constants.QUANTITY_KEY).getInt())
        .multiply(purchOrderLine.getPropertyValue(Constants.PURCHPRICE_KEY).getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      refundAmount = refundAmount.add(purchAmount);
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);
    //create purch invoice if refund is negative
    if (refundAmount.floatValue() < 0) {
      String label = Constants.PURCHINVOICE_VERTEX_LABEL;
      Properties properties = new Properties();

      properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
      properties.set(
        Constants.DATE_KEY, ticket.getPropertyValue(Constants.CREATEDATE_KEY).getDate());
      String bid = createBusinessIdentifier(
        currentId++, Constants.PURCHINVOICE_ACRONYM);
      properties.set(Constants.SOURCEID_KEY, Constants.CIT_ACRONYM + "_" + bid);
      properties.set(Constants.EXPENSE_KEY, refundAmount);
      properties.set(Constants.TEXT_KEY, Constants.TEXT_CONTENT + ticket.getId());

      Vertex purchInvoice = newVertex(label, properties);

      newEdge(Constants.CREATEDFOR_EDGE_LABEL, purchInvoice.getId(), purchOrderId);
    }
  }

  /**
   * Returns set of all vertices with the given label.
   *
   * @param transaction the graph transaction containing the vertices
   * @param label the label to be searched on
   * @return set of vertices with the given label
   */
  private Set<Vertex> getVertexByLabel(GraphTransaction transaction, String label) {
    Set<Vertex> vertices = Sets.newHashSet();

    for (Vertex vertex : transaction.getVertices()) {
      if (vertex.getLabel().equals(label)) {
        vertices.add(vertex);
      }
    }
    return vertices;
  }

  /**
   * Returns set of all edges with the given label.
   *
   * @param transaction the graph transaction containing the edges
   * @param label the label to be searched on
   * @return set of edges with the given label
   */
  private Set<Edge> getEdgesByLabel(GraphTransaction transaction, String label) {
    Set<Edge> edges = Sets.newHashSet();
    for (Edge edge : transaction.getEdges()) {
      if (edge.getLabel().equals(label)) {
        edges.add(edge);
      }
    }
    return edges;
  }

  /**
   * Returns set of all purch order lines where the source id is equal to the given purch order id.
   *
   * @param purchOrderId gradoop id of the purch order
   * @return set of purch order lines
   */
  private Set<Edge> getPurchOrderLinesByPurchOrder(GradoopId purchOrderId) {
    Set<Edge> currentPurchOrderLines = Sets.newHashSet();
    for (Edge purchOrderLine : purchOrderLines) {
      if (purchOrderId.equals(purchOrderLine.getSourceId())) {
        currentPurchOrderLines.add(purchOrderLine);
      }
    }
    return currentPurchOrderLines;
  }

  /**
   * Returns all purch order lines which correspond to the given sales order line id.
   *
   * @param salesOrderLineId gradoop id of the sales order line
   * @return set of sales oder lines
   */
  private Edge getCorrespondingPurchOrderLine(GradoopId salesOrderLineId) {
    for (Edge purchOrderLine : purchOrderLines) {
      if (purchOrderLine.getPropertyValue("salesOrderLine").getString()
        .equals(salesOrderLineId.toString())) {
        return purchOrderLine;
      }
    }
    return null;
  }

  /**
   * Returns the sales order line which correspond to the given purch order line id.
   *
   * @param purchOrderLineId gradoop id of the purch order line
   * @return sales order line
   */
  private Edge getCorrespondingSalesOrderLine(GradoopId purchOrderLineId) {
    for (Edge salesOrderLine : salesOrderLines) {
      if (salesOrderLine.getPropertyValue("purchOrderLine").getString()
        .equals(purchOrderLineId.toString())) {
        return salesOrderLine;
      }
    }
    return null;
  }

  /**
   * Returns the vertex to the given customer id.
   *
   * @param id gradoop id of the customer
   * @return the vertex representing a customer
   */
  private Vertex getCustomerById(GradoopId id) {
    for (Vertex vertex : customers) {
      if (vertex.getId().equals(id)) {
        return vertex;
      }
    }
    return null;
  }

  /**
   * Returns the vertex to the given employee id.
   *
   * @param id gradoop id of the employee
   * @return the vertex representing an employee
   */
  private Vertex getEmployeeById(GradoopId id) {
    for (Vertex vertex : employees) {
      if (vertex.getId().equals(id)) {
        return vertex;
      }
    }
    return null;
  }

  /**
   * Creates, if not already existing, and returns the corresponding user vertex to the given
   * employee id.
   *
   * @param employeeId gradoop id of the employee
   * @return the vertex representing an user
   */
  private Vertex getUserFromEmployeeId(GradoopId employeeId) {
    if (masterDataMap.containsKey(employeeId)) {
      return masterDataMap.get(employeeId);
    } else {
      //create properties
      Properties properties;
      Vertex employee = getEmployeeById(employeeId);
      properties = employee.getProperties();
      String sourceIdKey = properties.get(Constants.SOURCEID_KEY).getString();
      sourceIdKey = sourceIdKey.replace(Constants.EMPLOYEE_ACRONYM, Constants.USER_ACRONYM);
      sourceIdKey = sourceIdKey.replace(Constants.ERP_ACRONYM, Constants.CIT_ACRONYM);
      properties.set(Constants.SOURCEID_KEY, sourceIdKey);
      properties.set(Constants.ERPEMPLNUM_KEY, employee.getId().toString());
      String email = properties.get(Constants.NAME_KEY).getString();
      email = email.replace(" ", ".").toLowerCase();
      email += "@biiig.org";
      properties.set(Constants.EMAIL_KEY, email);
      //create the vertex and store it in a map for fast access
      Vertex user = vertexFactory.createVertex(Constants.USER_VERTEX_LABEL, properties, graphIds);
      masterDataMap.put(employeeId, user);
      userMap.put(user.getId(), user.getPropertyValue(Constants.QUALITY_KEY).getFloat());

      newEdge(Constants.SAMEAS_EDGE_LABEL, user.getId(), employeeId);
      return user;
    }
  }

  /**
   * Creates, if not already existing, and returns the corresponding client vertex to the given
   * customer id.
   *
   * @param customerId gradoop id of the customer
   * @return the vertex representing a client
   */
  private Vertex getClientFromCustomerId(GradoopId customerId) {
    if (masterDataMap.containsKey(customerId)) {
      return masterDataMap.get(customerId);
    } else {
      //create properties
      Properties properties;
      Vertex customer = getCustomerById(customerId);
      properties = customer.getProperties();
      String sourceIdKey = properties.get(Constants.SOURCEID_KEY).getString();
      sourceIdKey = sourceIdKey.replace(Constants.CUSTOMER_ACRONYM, Constants.CLIENT_ACRONYM);
      sourceIdKey = sourceIdKey.replace(Constants.ERP_ACRONYM, Constants.CIT_ACRONYM);
      properties.set(Constants.SOURCEID_KEY, sourceIdKey);
      properties.set(Constants.ERPCUSTNUM_KEY, customer.getId().toString());
      properties.set(Constants.CONTACTPHONE_KEY, "0123456789");
      properties.set(Constants.ACCOUNT_KEY, "CL" + customer.getId().toString());
      //create the vertex and store it in a map for fast access
      Vertex client = vertexFactory.createVertex(
        Constants.CLIENT_VERTEX_LABEL, properties, graphIds);
      masterDataMap.put(customerId, client);

      newEdge(Constants.SAMEAS_EDGE_LABEL, client.getId(), customerId);
      return client;
    }
  }

  /**
   * Returns set of vertices which contains all in this process created master data.
   *
   * @return set of vertices
   */
  private Set<Vertex> getMasterData() {
    return Sets.newHashSet(masterDataMap.values());
  }

  /**
   * Creates a map from label and source id of an edge to a set of all edges which meet the
   * criteria.
   *
   * @param transaction the graph transaction containing all the edges
   * @return HashMap from (String, GradoopId) -> Set(Edge)
   */
  private Map<Tuple2<String, GradoopId>, Set<Edge>> createEdgeMap(GraphTransaction transaction) {
    Map<Tuple2<String, GradoopId>, Set<Edge>> edgeMap = Maps.newHashMap();
    Set<Edge> edges;
    Tuple2<String, GradoopId> key;
    for (Edge edge : transaction.getEdges()) {
      edges = Sets.newHashSet();
      key = new Tuple2<>(edge.getLabel(), edge.getSourceId());
      if (edgeMap.containsKey(key)) {
        edges.addAll(edgeMap.get(key));
      }
      edges.add(edge);
      edgeMap.put(key, edges);
    }
    return edgeMap;
  }
}
