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
package com.cloudera.nav.sdk.model.entities;

import com.cloudera.nav.sdk.model.HiveIdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Represents a Hive table; uniquely identified by the source id, database name,
 * and table name
 */
@MClass(model = "hv_table")
public class HiveTable extends Entity {

  private final String DATABASE_NAME = "databaseName";
  private final String TABLE_NAME = "tableName";

  @MProperty
  private String databaseName;

  @MProperty
  private String tableName;

  public HiveTable() {
    setSourceType(SourceType.HIVE);
    setEntityType(EntityType.TABLE);
  }

  public HiveTable(String sourceId, String db, String table) {
    this();
    setSourceId(sourceId);
    setDatabaseName(db);
    setTableName(table);
  }

  public HiveTable(String id) {
    this();
    setIdentity(id);
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  /**
   * @return the table name. This aliases getName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Change the table name. This aliases setName
   * @param tableName
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public Map<String, String> getIdAttrsMap() {
    return ImmutableMap.of(
        DATABASE_NAME, this.getDatabaseName(),
        TABLE_NAME, this.getTableName());
  }
}
