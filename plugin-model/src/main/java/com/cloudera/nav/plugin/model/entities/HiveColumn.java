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
  public String generateId() {
    return HiveIdGenerator.generateColumnId(getSourceId(), getDatabaseName(),
        getTableName(), getColumnName());
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
