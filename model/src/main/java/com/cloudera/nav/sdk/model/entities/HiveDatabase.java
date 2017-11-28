// (c) Copyright 2017 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.sdk.model.entities;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Represents a Hive Database; uniquely identified by the source id, database
 * name
 */
@MClass(model = "hv_database")
public class HiveDatabase extends Entity {

  private final String DATABASE_NAME = "databaseName";

  @MProperty
  private String databaseName;

  public HiveDatabase() {
    setSourceType(SourceType.HIVE);
    setEntityType(EntityType.DATABASE);
  }

  public HiveDatabase(String id) {
    this();
    setIdentity(id);
  }

  public HiveDatabase(String sourceId, String db) {
    this();
    setSourceId(sourceId);
    setDatabaseName(db);
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  @Override
  public Map<String, String> getIdAttrsMap() {
    return ImmutableMap.of(
        DATABASE_NAME, this.getDatabaseName());
  }
}