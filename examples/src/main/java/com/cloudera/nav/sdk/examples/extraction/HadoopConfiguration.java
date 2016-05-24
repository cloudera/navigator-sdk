package com.cloudera.nav.sdk.examples.extraction;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/** Class to de-serialize XML file to Hadoop configuration. */
@XmlRootElement(name = "configuration")
public class HadoopConfiguration {

  @XmlRootElement(name = "property")
  public static class ConfigProperty {
    @XmlElement(name = "name")
    public String name;
    @XmlElement(name = "value")
    public String value;
    @XmlElement(name = "source")
    public String source;
  }

  @XmlElement(name = "property")
  public List<ConfigProperty> property;
}
