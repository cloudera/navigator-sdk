package com.cloudera.nav.sdk.examples.extraction;

/**
 * A builder to build EntityQuery
 */
public class EntityQueryBuilder {
  private String type;
  private String sourceType;
  private String principal;
  private String startTimeMin;
  private String startTimeMax;
  private String customQuery;

  public EntityQuery buildSolrQuery() {
    return new EntityQuery(type, sourceType, principal, startTimeMin, startTimeMax, customQuery);
  }

  public EntityQueryBuilder type(String type) {
    this.type = type;
    return this;
  }

  public EntityQueryBuilder sourceType(String sourceType) {
    this.sourceType = sourceType;
    return this;
  }

  public EntityQueryBuilder principal(String principal) {
    this.principal = principal;
    return this;
  }

  public EntityQueryBuilder startTimeMin(String startTimeMin) {
    this.startTimeMin = startTimeMin;
    return this;
  }

  public EntityQueryBuilder startTimeMax(String startTimeMax) {
    this.startTimeMax = startTimeMax;
    return this;
  }

  public EntityQueryBuilder customQuery(String customQuery) {
    this.customQuery = customQuery;
    return this;
  }
}
