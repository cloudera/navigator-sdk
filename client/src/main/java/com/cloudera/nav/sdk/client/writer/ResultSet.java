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

package com.cloudera.nav.sdk.client.writer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.Collection;

import org.apache.commons.collections.CollectionUtils;

/**
 * Metadata writer results. Reports the number of entities and relations
 * written and also any errors that occurred.
 */
public class ResultSet {

  /**
   * Count of new/updated metadata objects and also errors that occurred during
   * processing
   */
  public static class UpdateWrapper {
    private final int count;
    private final Collection<String> errors;

    @JsonCreator
    public UpdateWrapper(@JsonProperty("count") int count,
                         @JsonProperty("errors") Collection<String> errors) {
      this.count = count;
      this.errors = ImmutableList.copyOf(errors);
    }

    /**
     * @return number of metadata entities successfully processed by the server
     */
    public int getCount() {
      return count;
    }

    /**
     * Errors occurred during metadata write
     */
    public Collection<String> getErrors() {
      return errors;
    }
  }

  private final UpdateWrapper entities;
  private final UpdateWrapper relations;

  @JsonCreator
  public ResultSet(@JsonProperty("entities") UpdateWrapper entities,
                   @JsonProperty("relations") UpdateWrapper relations) {
    this.entities = entities;
    this.relations = relations;
  }

  public UpdateWrapper getEntities() {
    return entities;
  }

  public UpdateWrapper getRelations() {
    return relations;
  }


  public boolean hasErrors() {
    return (getEntities() != null && CollectionUtils.isNotEmpty(getEntities()
        .getErrors())) || (getRelations() != null && CollectionUtils
        .isNotEmpty(getRelations().getErrors()));
  }

  @Override
  public String toString() {
    String base = "\nRead %d %s.";
    String rs = "";
    if (getEntities() != null) {
      rs += String.format(base, getEntities().getCount(), "entities");
      if (CollectionUtils.isNotEmpty(getEntities().getErrors())) {
        rs += "\nEntity errors (position : message) -\n" +
            Joiner.on("\n").join(getEntities().getErrors());
      }
    }
    if (getRelations() != null) {
      rs += String.format(base, getRelations().getCount(), "relations");
      if (CollectionUtils.isNotEmpty(getRelations().getErrors())) {
        rs += "\nRelation errors (position : message) -\n" +
            Joiner.on("\n").join(getRelations().getErrors());
      }
    }
    return rs;
  }
}
