//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.11 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2016.11.04 at 10:32:38 AM CET 
//


package org.gradoop.flink.io.impl.csv.pojos;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElements;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for staticorreference complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="staticorreference"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;choice&gt;
 *         &lt;sequence&gt;
 *           &lt;element ref="{http://www.gradoop.org/flink/io/impl/csv/pojos}static"/&gt;
 *           &lt;element ref="{http://www.gradoop.org/flink/io/impl/csv/pojos}ref" maxOccurs="unbounded" minOccurs="0"/&gt;
 *           &lt;element ref="{http://www.gradoop.org/flink/io/impl/csv/pojos}reference" maxOccurs="unbounded" minOccurs="0"/&gt;
 *         &lt;/sequence&gt;
 *         &lt;sequence&gt;
 *           &lt;choice maxOccurs="unbounded"&gt;
 *             &lt;element ref="{http://www.gradoop.org/flink/io/impl/csv/pojos}ref"/&gt;
 *             &lt;element ref="{http://www.gradoop.org/flink/io/impl/csv/pojos}reference"/&gt;
 *           &lt;/choice&gt;
 *         &lt;/sequence&gt;
 *       &lt;/choice&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "staticorreference", propOrder = {
    "_static",
    "ref",
    "reference",
    "refOrReference"
})
@XmlSeeAlso({
    Source.class,
    Target.class
})
public class Staticorreference
    implements Serializable
{

    private final static long serialVersionUID = 1L;
    @XmlElement(name = "static")
    protected Static _static;
    protected List<Ref> ref;
    protected List<Reference> reference;
    @XmlElements({
        @XmlElement(name = "ref", type = Ref.class),
        @XmlElement(name = "reference", type = Reference.class)
    })
    protected List<Serializable> refOrReference;

    /**
     * Gets the value of the static property.
     * 
     * @return
     *     possible object is
     *     {@link Static }
     *     
     */
    public Static getStatic() {
        return _static;
    }

    /**
     * Sets the value of the static property.
     * 
     * @param value
     *     allowed object is
     *     {@link Static }
     *     
     */
    public void setStatic(Static value) {
        this._static = value;
    }

    /**
     * Gets the value of the ref property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the ref property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getRef().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link Ref }
     * 
     * 
     */
    public List<Ref> getRef() {
        if (ref == null) {
            ref = new ArrayList<Ref>();
        }
        return this.ref;
    }

    /**
     * Gets the value of the reference property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the reference property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getReference().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link Reference }
     * 
     * 
     */
    public List<Reference> getReference() {
        if (reference == null) {
            reference = new ArrayList<Reference>();
        }
        return this.reference;
    }

    /**
     * Gets the value of the refOrReference property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the refOrReference property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getRefOrReference().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link Ref }
     * {@link Reference }
     * 
     * 
     */
    public List<Serializable> getRefOrReference() {
        if (refOrReference == null) {
            refOrReference = new ArrayList<Serializable>();
        }
        return this.refOrReference;
    }

}
