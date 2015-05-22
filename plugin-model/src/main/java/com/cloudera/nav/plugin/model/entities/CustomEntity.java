// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.model.entities;

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MProperty;

/**
 * Abstract base classes for creating custom entities defined by non-Hadoop
 * applications
 */
public abstract class CustomEntity extends Entity {

  private String namespace;

  /**
   * @return Navigator assigned namespace for the custom entity
   */
  @MProperty(required=true)
  public String getNamespace() {
    return namespace;
  }

  @Override
  @MProperty(required=true)
  public SourceType getSourceType() {
    return SourceType.PLUGIN;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }
}
