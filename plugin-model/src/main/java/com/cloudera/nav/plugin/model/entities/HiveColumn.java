package com.cloudera.nav.plugin.model.entities;

import com.cloudera.nav.plugin.model.annotations.MProperty;

public class HiveColumn extends HiveTable {

  private String columnName;

  /**
   * A Hive column is identified by the source id, database name, table name,
   * and column name
   *
   * @return
   */
  @Override
  protected String[] getIdComponents() {
    return new String[]{getSourceId(), getDatabaseName(), getTableName(),
        getColumnName()};
  }

  @MProperty
  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  @Override
  @MProperty(attribute = "originalName")
  public String getName() {
    return getColumnName();
  }

  @Override
  public void setName(String name) {
    setColumnName(name);
  }
}
