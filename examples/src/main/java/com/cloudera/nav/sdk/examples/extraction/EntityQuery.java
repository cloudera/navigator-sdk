package com.cloudera.nav.sdk.examples.extraction;

import org.apache.commons.lang.StringUtils;

/**
 * A solr query class
 */
public class EntityQuery {

  private String type;
  private String sourceType;
  private String principal;
  private String startTimeMin;
  private String startTimeMax;
  private String customQuery;

  public EntityQuery(String type, String sourceType, String principal, String startTimeMin, String startTimeMax, String customQuery) {
    this.type = type;
    this.sourceType = sourceType;
    this.principal = principal;
    this.startTimeMin = startTimeMin;
    this.startTimeMax = startTimeMax;
    this.customQuery = customQuery;
  }

  public String getType() {
    return StringUtils.isEmpty(type) ? "operation_execution" : type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getSourceType() {
    return StringUtils.isEmpty(sourceType) ? "HIVE" : sourceType;
  }

  public void setSourceType(String sourceType) {
    this.sourceType = sourceType;
  }

  public String getPrincipal() {
    return principal;
  }

  public void setPrincipal(String principal) {
    this.principal = principal;
  }

  public String getStartTimeMin() {
    return startTimeMin;
  }

  public void setStartTimeMin(String startTimeMin) {
    this.startTimeMin = startTimeMin;
  }

  public String getStartTimeMax() {
    return startTimeMax;
  }

  public void setStartTimeMax(String startTimeMax) {
    this.startTimeMax = startTimeMax;
  }

  public String getCustomQuery() {
    return customQuery;
  }

  public void setCustomQuery(String customQuery) {
    this.customQuery = customQuery;
  }

  public String toString() {
    String s;
    if (StringUtils.isEmpty(getCustomQuery())) {
      s = "type:" + getType() + " AND sourceType:" + getSourceType();
      s = StringUtils.isEmpty(getPrincipal()) ? s : s + " AND principal:" + getPrincipal();
    }
    else {
      s = getCustomQuery();
    }
    return s + " AND started:[" + getStartTimeMin() + " TO " + getStartTimeMax() + "]";
  }
}
