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

import java.util.Collection;
import java.util.Collections;

/**
 * many-to-many relation between sources and targets of data movement
 * (e.g., input data to MR operation and output data from MR operation)
 */
public final class DataFlowRelation extends Relation {

  public static class Builder<T extends Builder<T>> extends Relation.Builder<T> {

    protected Builder() {
      super(RelationType.DATA_FLOW);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T self() {
      return (T) this;
    }

    @Override
    public DataFlowRelation build() {
      return new DataFlowRelation(this);
    }

    public T sourceIds(Collection<String> sourceIds) {
      return ep1Ids(sourceIds);
    }

    public T sourceId(String sourceId) {
      return sourceIds(Collections.singleton(sourceId));
    }

    public T targetIds(Collection<String> targetIds) {
      return ep2Ids(targetIds);
    }

    public T targetId(String targetId) {
      return targetIds(Collections.singleton(targetId));
    }

    public T sourceTypeOfSource(SourceType sourceType) {
      return ep1SourceType(sourceType);
    }

    public T sourceTypeOfTarget(SourceType sourceType) {
      return ep2SourceType(sourceType);
    }

    public T source(Entity src) {
      return sources(ImmutableList.of(src));
    }

    public T sources(Collection<Entity> src) {
      return ep1(src);
    }

    public T target(Entity tgt) {
      return targets(ImmutableList.of(tgt));
    }

    public T targets(Collection<Entity> tgt) {
      return ep2(tgt);
    }
  }

  @SuppressWarnings("rawtypes")
  public static Builder<?> builder() {
    return new Builder();
  }

  private DataFlowRelation(Builder<?> builder) {
    super(builder);
  }

  public Collection<String> getSourceIds() {
    return getEp1Ids();
  }

  public Collection<String> getTargetIds() {
    return getEp2Ids();
  }

  public SourceType getSourceTypeOfSource() {
    return getEp1SourceType();
  }

  public SourceType getSourceTypeOfTarget() {
    return getEp2SourceType();
  }
}
