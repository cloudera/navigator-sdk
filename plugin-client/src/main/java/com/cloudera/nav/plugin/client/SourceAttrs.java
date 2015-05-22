// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.client;

import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Used by NavApiClient for JSON deserialization because Source is
 * immutable
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class SourceAttrs {

  private String clusterName;
  private String originalName;
  private SourceType sourceType;
  private String sourceUrl;
  private String identity;

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public String getOriginalName() {
    return originalName;
  }

  public void setOriginalName(String originalName) {
    this.originalName = originalName;
  }

  public String getSourceUrl() {
    return sourceUrl;
  }

  public void setSourceUrl(String sourceUrl) {
    this.sourceUrl = sourceUrl;
  }

  public String getIdentity() {
    return identity;
  }

  public void setIdentity(String identity) {
    this.identity = identity;
  }

  public SourceType getSourceType() {
    return sourceType;
  }

  public void setSourceType(SourceType sourceType) {
    this.sourceType = sourceType;
  }

  public Source createSource() {
    return new Source(getOriginalName(), getSourceType(),
        getClusterName(), getSourceUrl(), getIdentity());
  }
}
