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

package org.gradoop.flink.datagen.foodbroker.functions.process;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.functions.masterdata.Customer;
import org.gradoop.flink.datagen.foodbroker.functions.masterdata.Employee;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.math.BigDecimal;
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
    employees = getRuntimeContext().getBroadcastVariable(Employee.CLASS_NAME);
    customers = getRuntimeContext().getBroadcastVariable(Customer.CLASS_NAME);
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
    deliveryNotes = getVertexByLabel(transaction, "DeliveryNote");
    salesOrderLines = getEdgesByLabel(transaction, "SalesOrderLine");
    purchOrderLines = getEdgesByLabel(transaction, "PurchOrderLine");
    salesOrder = getVertexByLabel(transaction, "SalesOrder").iterator().next();

    //create new graph head
    graphHead = graphHeadFactory.createGraphHead();
    graphIds = new GradoopIdSet();
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
    Set<Edge> purchOrderLines;
    Set<Edge> badSalesOrderLines;

    for (Vertex deliveryNote : deliveryNotes) {
      influencingMasterQuality = Lists.newArrayList();
      badSalesOrderLines = Sets.newHashSet();
      //get the corresponding purch order and purch order lines
      purchOrderId = getEdgeTargetId("contains", deliveryNote.getId());
      purchOrderLines = this.getPurchOrderLinesByPurchOrder(purchOrderId);

      for (Edge purchOrderLine : purchOrderLines){
        influencingMasterQuality.add(productQualityMap.get(purchOrderLine.getTargetId()));
      }
      int containedProducts = influencingMasterQuality.size();
      // increase relative influence of vendor and logistics
      for(int i = 1; i <= containedProducts / 2; i++){
        influencingMasterQuality.add(getEdgeTargetQuality("operatedBy",
          deliveryNote.getId(), Constants.LOGISTIC_MAP));
        influencingMasterQuality.add(getEdgeTargetQuality("placedAt",
          purchOrderId, Constants.VENDOR_MAP));
      }
      if (config.happensTransitionConfiguration(influencingMasterQuality, "Ticket",
        "badQualityProbability")) {

        for (Edge purchOrderLine : purchOrderLines) {
          badSalesOrderLines.add(getCorrespondingSalesOrderLine(
            purchOrderLine.getId()));
        }

        Vertex ticket = newTicket("bad quality", deliveryNote
          .getPropertyValue("date").getLong());
        grantSalesRefund(badSalesOrderLines, ticket);
        claimPurchRefund(purchOrderLines, ticket);
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
      if (deliveryNote.getPropertyValue("date").getLong() >
        salesOrder.getPropertyValue("deliveryDate").getLong()) {
        lateSalesOrderLines.addAll(salesOrderLines);
      }
    }

    // If we have late sales order lines
    if (!lateSalesOrderLines.isEmpty()) {
      // Collect the respective late purch order lines
      Set<Edge> latePurchOrderLines = Sets.newHashSet();
      for (Edge salesOrderLine : lateSalesOrderLines) {
        latePurchOrderLines
          .add(getCorrespondingPurchOrderLine(salesOrderLine.getId()));
      }
      Calendar calendar = Calendar.getInstance();
      calendar.setTimeInMillis(salesOrder.getPropertyValue("deliveryDate").getLong());
      calendar.add(Calendar.DATE, 1);
      long createdDate = calendar.getTimeInMillis();

      // Create ticket and process refunds
      Vertex ticket = newTicket("late delivery", createdDate);
      grantSalesRefund(lateSalesOrderLines, ticket);
      claimPurchRefund(latePurchOrderLines, ticket);
    }
  }


  private Vertex newTicket(String problem, long createdAt) {
    String label = "Ticket";
    Properties properties = new Properties();

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("createdAt", createdAt);
    properties.set("problem", problem);
    properties.set("erpSoNum", salesOrder.getId().toString());

    GradoopId employeeId = getNextEmployee();
    GradoopId customerId = getEdgeTargetId("receivedFrom", salesOrder.getId());

    Vertex ticket = newVertex(label, properties);

    newEdge("concerns", ticket.getId(), salesOrder.getId());

    Vertex user = getUserFromEmployeeId(employeeId);

    newEdge("createdBy", ticket.getId(), user.getId());

    employeeId = getNextEmployee();
    user = getUserFromEmployeeId(employeeId);

    newEdge("allocatedTo", ticket.getId(), user.getId());

    Vertex client = getClientFromCustomerId(customerId);

    newEdge("openedBy", ticket.getId(), client.getId());

    return ticket;
  }


  private void grantSalesRefund(Set<Edge> salesOrderLines, Vertex ticket) {
    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality("allocatedTo", ticket
      .getId(), Constants.USER_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality("receivedFrom",
      salesOrder.getId(), Constants.CUSTOMER_MAP));

    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(influencingMasterQuality, "Ticket",
        "salesRefund");
    BigDecimal refundAmount = BigDecimal.ZERO;
    BigDecimal salesAmount;

    for (Edge salesOrderLine : salesOrderLines) {
      salesAmount = BigDecimal.valueOf(salesOrderLine.getPropertyValue(
        "quantity").getInt())
        .multiply(salesOrderLine.getPropertyValue("salesPrice").getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      refundAmount = refundAmount.add(salesAmount);
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);

    if (refundAmount.floatValue() < 0) {
      String label = "SalesInvoice";

      Properties properties = new Properties();
      properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
      properties.set("date", ticket.getPropertyValue("createdAt").getLong());
      String bid = createBusinessIdentifier(
        currentId++, Constants.SALESINVOICE_ACRONYM);
      properties.set("num", bid);
      properties.set("revenue", refundAmount);
      properties.set("text", "*** TODO @ ComplaintHandlingOld ***");

      Vertex salesInvoice = newVertex(label, properties);

      newEdge("createdFor", salesInvoice.getId(), salesOrder.getId());
    }
  }

  private void claimPurchRefund(Set<Edge> purchOrderLines, Vertex ticket) {
    GradoopId purchOrderId = purchOrderLines.iterator().next().getSourceId();

    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality("allocatedTo", ticket
      .getId(), Constants.USER_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality("placedAt",
      purchOrderId, Constants.VENDOR_MAP));

    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(influencingMasterQuality, "Ticket",
        "purchRefund");
    BigDecimal refundAmount = BigDecimal.ZERO;
    BigDecimal purchAmount;

    for (Edge purchOrderLine : purchOrderLines) {
      purchAmount = BigDecimal.valueOf(purchOrderLine.getPropertyValue(
        "quantity").getInt())
        .multiply(purchOrderLine.getPropertyValue("purchPrice").getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      refundAmount = refundAmount.add(purchAmount);
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);

    if (refundAmount.floatValue() < 0) {
      String label = "PurchInvoice";
      Properties properties = new Properties();

      properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
      properties.set("date", ticket.getPropertyValue("createdAt").getLong());
      String bid = createBusinessIdentifier(
        currentId++, Constants.PURCHINVOICE_ACRONYM);
      properties.set("num", bid);
      properties.set("expense", refundAmount);
      properties.set("text", "*** TODO @ ComplaintHandlingOld ***");

      Vertex purchInvoice = newVertex(label, properties);

      newEdge("createdFor", purchInvoice.getId(), purchOrderId);
    }
  }


  private Set<Vertex> getVertexByLabel(GraphTransaction transaction, String label) {
    Set<Vertex> vertices = Sets.newHashSet();

    for (Vertex vertex : transaction.getVertices()) {
      if (vertex.getLabel().equals(label)) {
        vertices.add(vertex);
      }
    }
    return vertices;
  }

  private Set<Edge> getEdgesByLabel(GraphTransaction transaction, String label) {
    Set<Edge> edges = Sets.newHashSet();
    for (Edge edge : transaction.getEdges()) {
      if (edge.getLabel().equals(label)) {
        edges.add(edge);
      }
    }
    return edges;
  }

  private Set<Edge> getPurchOrderLinesByPurchOrder(GradoopId purchOrderId) {
    Set<Edge> purchOrderLines = Sets.newHashSet();
    for (Edge purchOrderLine : this.purchOrderLines) {
      if (purchOrderId.equals(purchOrderLine.getSourceId())) {
        purchOrderLines.add(purchOrderLine);
      }
    }
    return purchOrderLines;
  }

  private Edge getCorrespondingPurchOrderLine(GradoopId salesOrderLineId) {
    for (Edge purchOrderLine : this.purchOrderLines) {
      if (purchOrderLine.getPropertyValue("salesOrderLine").getString()
        .equals(salesOrderLineId.toString())) {
        return purchOrderLine;
      }
    }
    return null;
  }

  private Edge getCorrespondingSalesOrderLine(GradoopId purchOrderLineId) {
    for (Edge salesOrderLine : this.salesOrderLines) {
      if (salesOrderLine.getPropertyValue("purchOrderLine").getString()
        .equals(purchOrderLineId.toString())) {
        return salesOrderLine;
      }
    }
    return null;
  }

  private Vertex getCustomerById(GradoopId id) {
    for (Vertex vertex : customers) {
      if (vertex.getId().equals(id)) {
        return vertex;
      }
    }
    return null;
  }

  private Vertex getEmployeeById(GradoopId id) {
    for (Vertex vertex : employees) {
      if (vertex.getId().equals(id)) {
        return vertex;
      }
    }
    return null;
  }

  private Vertex getUserFromEmployeeId(GradoopId employeeId) {
    if (masterDataMap.containsKey(employeeId)) {
      return masterDataMap.get(employeeId);
    } else {
      Properties properties;
      Vertex employee = getEmployeeById(employeeId);
      properties = employee.getProperties();
      properties.set("erpEmplNum", employee.getId().toString());
      String email = properties.get("name").getString();
      email = email.replace(" ", ".").toLowerCase();
      email += "@biiig.org";
      properties.set("email", email);
      properties.remove("num");
      properties.remove("sid");
      Vertex user = vertexFactory.createVertex("User", properties, graphIds);

      masterDataMap.put(employeeId, user);
      userMap.put(user.getId(), user.getPropertyValue(Constants.QUALITY).getFloat());

      newEdge("sameAs", user.getId(), employeeId);
      return user;
    }
  }

  private Vertex getClientFromCustomerId(GradoopId customerId) {
    if (masterDataMap.containsKey(customerId)) {
      return masterDataMap.get(customerId);
    } else {
      Properties properties;
      Vertex customer = getCustomerById(customerId);
      properties = customer.getProperties();
      properties.set("erpCustNum", customer.getId().toString());
      properties.set("contactPhone", "0123456789");
      properties.set("account", "CL" + customer.getId().toString());
      properties.remove("num");
      properties.remove("sid");
      Vertex client = vertexFactory.createVertex("Client", properties, graphIds);

      masterDataMap.put(customerId, client);

      newEdge("sameAs", client.getId(), customerId);
      return client;
    }
  }


  private Set<Vertex> getMasterData() {
    return Sets.newHashSet(masterDataMap.values());
  }


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
