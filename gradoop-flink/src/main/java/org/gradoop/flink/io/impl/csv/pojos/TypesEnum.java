//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.11 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2016.11.04 at 10:32:38 AM CET 
//


package org.gradoop.flink.io.impl.csv.pojos;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for typesEnum.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * <p>
 * <pre>
 * &lt;simpleType name="typesEnum"&gt;
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string"&gt;
 *     &lt;enumeration value="Integer"/&gt;
 *     &lt;enumeration value="Long"/&gt;
 *     &lt;enumeration value="Float"/&gt;
 *     &lt;enumeration value="Double"/&gt;
 *     &lt;enumeration value="String"/&gt;
 *     &lt;enumeration value="Boolean"/&gt;
 *   &lt;/restriction&gt;
 * &lt;/simpleType&gt;
 * </pre>
 * 
 */
@XmlType(name = "typesEnum")
@XmlEnum
public enum TypesEnum {

    @XmlEnumValue("Integer")
    INTEGER("Integer"),
    @XmlEnumValue("Long")
    LONG("Long"),
    @XmlEnumValue("Float")
    FLOAT("Float"),
    @XmlEnumValue("Double")
    DOUBLE("Double"),
    @XmlEnumValue("String")
    STRING("String"),
    @XmlEnumValue("Boolean")
    BOOLEAN("Boolean");
    private final String value;

    TypesEnum(String v) {
        value = v;
    }

    public String value() {
        return value;
    }

    public static TypesEnum fromValue(String v) {
        for (TypesEnum c: TypesEnum.values()) {
            if (c.value.equals(v)) {
                return c;
            }
        }
        throw new IllegalArgumentException(v);
    }

}