// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.model.entities;

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.annotations.MProperty;

@MClass
public class HiveTable extends Entity {

  private String databaseName;
  private String tableName;

  /**
   * A Hive table is identified by the source id, database name, and table name
   *
   * @return
   */
  @Override
  protected String[] getIdComponents() {
    return new String[]{getSourceId(), getDatabaseName(), getTableName()};
  }

  @Override
  @MProperty(required=true)
  public EntityType getType() {
    return EntityType.TABLE;
  }

  @Override
  @MProperty(required=true)
  public SourceType getSourceType() {
    return SourceType.HIVE;
  }

  @MProperty
  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  @MProperty
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Override
  @MProperty(attribute = "originalName")
  public String getName() {
    return getTableName();
  }

  @Override
  public void setName(String name) {
    setTableName(name);
  }
}
