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

import com.cloudera.nav.sdk.model.annotations.MProperty;

import java.util.Collection;

public abstract class SchemaElement extends Entity {

  @MProperty
  private String schemaName;
  @MProperty
  private String schemaNamespace;
  @MProperty
  private String fullDataType;
  @MProperty
  private String dataType;
  @MProperty
  private Collection<String> schemaAliases;

  /**
   * Avro schemas come with their own names separate from
   * dataset and field names
   */

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  /**
   * Schema namespace + name = schema full name
   * Avro only considers schemas with the same full name
   * to be conflicting
   */
  public String getSchemaNamespace() {
    return schemaNamespace;
  }

  public void setSchemaNamespace(String schemaNamespace) {
    this.schemaNamespace = schemaNamespace;
  }

  /**
   * Aliases for the schema name
   */
  public Collection<String> getSchemaAliases() {
    return schemaAliases;
  }

  public void setSchemaAliases(Collection<String> schemaAliases) {
    this.schemaAliases = schemaAliases;
  }

  /**
   * E.g., null, string, enum, union, fixed
   * The bare data type information
   */
  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  /**
   * Captures sub-type information and parametrized types
   * E.g., enum('x', 'y'), union(null, string), fixed(32)
   */
  public String getFullDataType() {
    return fullDataType;
  }

  public void setFullDataType(String fullDataType) {
    this.fullDataType = fullDataType;
  }
}
