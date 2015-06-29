
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
 * Represents a Hive column; uniquely identified by the source id, database name,
 * table name, and column name. Note that the source type, entity type, and
 * namespace should not be modified.
 */
@MClass
public class HiveColumn extends Entity {

  @MProperty
  private String databaseName;
  @MProperty
  private String tableName;

  public HiveColumn() {
    setSourceType(SourceType.HIVE);
    setEntityType(EntityType.FIELD);
    setNamespace(NAVIGATOR);
  }

  public HiveColumn(String sourceId, String db, String table, String column) {
    this();
    setSourceId(sourceId);
    setDatabaseName(db);
    setTableName(table);
    setColumnName(column);
    setIdentity(generateId());
  }
  /**
   * A Hive column is identified by the source id, database name, table name,
   * and column name
   *
   * @return the entity id for this Hive column
   */
  @Override
  public String generateId() {
    return HiveIdGenerator.generateColumnId(getSourceId(), getDatabaseName(),
        getTableName(), getColumnName());
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return the column name. Aliases {@link com.cloudera.nav.plugin.model.entities.HiveColumn#getName}
   */
  public String getColumnName() {
    return getName();
  }

  /**
   * Changes the column name. Aliases {@link com.cloudera.nav.plugin.model.entities.HiveColumn#setName}
   * @param columnName
   */
  public void setColumnName(String columnName) {
    setName(columnName);
  }

}
