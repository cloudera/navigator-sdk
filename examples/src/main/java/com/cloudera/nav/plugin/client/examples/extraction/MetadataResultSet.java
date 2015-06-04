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
package com.cloudera.nav.plugin.client.examples.extraction;

/**
 * Class for clients to get all updated Entities and Relations and a marker of
 * most recent extractorRunId's for each source, that can then be used in
 * determining future incremental updates.
 */
public class MetadataResultSet {
  private final String marker;
  private final IncrementalExtractIterable entities;
  private final IncrementalExtractIterable relations;

  public MetadataResultSet(String marker,
                           IncrementalExtractIterable entities,
                           IncrementalExtractIterable relations){
      this.marker = marker;
      this.entities = entities;
      this.relations = relations;
  }

  public String getMarker() {
    return marker;
  }

  public IncrementalExtractIterable getEntities() {
    return entities;
  }

  public IncrementalExtractIterable getRelations() {
      return relations;
  }
}
