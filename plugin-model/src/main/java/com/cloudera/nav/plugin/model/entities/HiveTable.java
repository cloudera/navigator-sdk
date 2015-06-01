/*
 * Copyright (c) 2015 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
