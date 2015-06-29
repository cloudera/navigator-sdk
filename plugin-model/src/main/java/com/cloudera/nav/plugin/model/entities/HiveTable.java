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

import com.cloudera.nav.plugin.model.HiveIdGenerator;
import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.annotations.MProperty;

/**
 * Represents a Hive table; uniquely identified by the source id, database name,
 * and table name
 */
@MClass
public class HiveTable extends Entity {

  @MProperty
  private String databaseName;

  public HiveTable() {
    setSourceType(SourceType.HIVE);
    setEntityType(EntityType.TABLE);
    setNamespace(NAVIGATOR);
  }

  public HiveTable(String sourceId, String db, String table) {
    this();
    setSourceId(sourceId);
    setDatabaseName(db);
    setTableName(table);
    setIdentity(generateId());
  }

  /**
   * A Hive table is identified by the source id, database name, and table name
   *
   * @return the entity id for this Hive table
   */
  @Override
  public String generateId() {
    return HiveIdGenerator.generateTableId(getSourceId(), getDatabaseName(),
        getTableName());
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
    return getName();
  }

  /**
   * Change the table name. This aliases setName
   * @param tableName
   */
  public void setTableName(String tableName) {
    setName(tableName);
  }
}
