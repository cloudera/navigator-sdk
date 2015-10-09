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
package com.cloudera.nav.sdk.model.relations;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.Collections;

/**
 * 1-to-many relationship between a logical entity and its physical
 * manifestations (e.g., Hive table and HDFS directory/files)
 */
public final class LogicalPhysicalRelation extends Relation {

  public static class Builder<T extends Builder<T>> extends Relation.Builder<T> {

    protected Builder() {
      super(RelationType.LOGICAL_PHYSICAL);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T self() {
      return (T) this;
    }

    @Override
    public LogicalPhysicalRelation build() {
      return new LogicalPhysicalRelation(this);
    }

    public T logicalId(String logicalId) {
      return ep1Ids(Collections.singleton(logicalId));
    }

    public T physicalIds(Collection<String> physicalIds) {
      return ep2Ids(physicalIds);
    }

    public T physicalId(String physicalId) {
      return physicalIds(Collections.singleton(physicalId));
    }

    public T sourceTypeOfLogical(SourceType sourceType) {
      return ep1SourceType(sourceType);
    }

    public T sourceTypeOfPhysical(SourceType sourceType) {
      return ep2SourceType(sourceType);
    }

    public T logical(Entity src) {
      return ep1(ImmutableList.of(src));
    }

    public T physical(Entity physical) {
      return physical(ImmutableList.of(physical));
    }

    public T physical(Collection<Entity> physical) {
      return ep2(physical);
    }
  }

  @SuppressWarnings("rawtypes")
  public static Builder<?> builder() {
    return new Builder();
  }

  private LogicalPhysicalRelation(Builder<?> builder) {
    super(builder);
  }

  public String getLogicalId() {
    return Iterables.getOnlyElement(getEp1Ids());
  }

  public Collection<String> getPhysicalIds() {
    return getEp2Ids();
  }

  public SourceType getSourceTypeOfLogical() {
    return getEp1SourceType();
  }

  public SourceType getSourceTypeOfPhysical() {
    return getEp2SourceType();
  }
}
