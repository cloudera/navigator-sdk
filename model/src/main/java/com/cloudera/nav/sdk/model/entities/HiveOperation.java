/*
 * Copyright (c) 2017 Cloudera, Inc.
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

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

@MClass(model="hv_query", validTypes = {EntityType.OPERATION})
public class HiveOperation extends Entity {
  private final String QUERY_TEXT = "queryText";

  @MProperty
  private String queryText;

  public HiveOperation() {
    setSourceType(SourceType.HIVE);
    setEntityType(EntityType.OPERATION);
  }

  public HiveOperation(String sourceId, String queryText) {
    this();
    setQueryText(queryText);
    setSourceId(sourceId);
  }

  public String getQueryText() {
    return queryText;
  }

  public void setQueryText(String queryText) {
    this.queryText = queryText;
  }

  public Map<String, String> getIdAttrsMap() {
    return ImmutableMap.of(QUERY_TEXT, this.getQueryText());
  }
}