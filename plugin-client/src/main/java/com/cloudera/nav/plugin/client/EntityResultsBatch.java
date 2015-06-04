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

package com.cloudera.nav.plugin.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/**
 * Wrapper class for deserialization for batch of Entity results.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EntityResultsBatch extends ResultsBatch<Map<String, Object>> {

  public List<Map<String, Object>> getEntities() {
    return getResults();
  }

  public void setEntities(List<Map<String, Object>> results) {
    setResults(results);
  }
}
