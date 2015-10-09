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
 * 1-to-many relation between parent and children (e.g., HDFS directory and
 * its subdirectories and files)
 */
public final class ParentChildRelation extends Relation {

  public static class Builder<T extends Builder<T>> extends Relation.Builder<T> {

    protected Builder() {
      super(RelationType.PARENT_CHILD);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T self() {
      return (T) this;
    }

    @Override
    public ParentChildRelation build() {
      return new ParentChildRelation(this);
    }

    public T parentId(String parentId) {
      return ep1Ids(Collections.singleton(parentId));
    }

    public T childrenIds(Collection<String> childrenIds) {
      return ep2Ids(childrenIds);
    }

    public T childId(String childId) {
      return childrenIds(Collections.singleton(childId));
    }

    public T sourceTypeOfParent(SourceType sourceType) {
      return ep1SourceType(sourceType);
    }

    public T sourceTypeOfChildren(SourceType sourceType) {
      return ep2SourceType(sourceType);
    }

    public T parent(Entity src) {
      return ep1(ImmutableList.of(src));
    }

    public T child(Entity child) {
      return children(ImmutableList.of(child));
    }

    public T children(Collection<Entity> children) {
      return ep2(children);
    }
  }

  @SuppressWarnings("rawtypes")
  public static Builder<?> builder() {
    return new Builder();
  }

  private ParentChildRelation(Builder<?> builder) {
    super(builder);
  }

  public String getParentId() {
    return Iterables.getOnlyElement(getEp1Ids());
  }

  public Collection<String> getChildrenIds() {
    return getEp2Ids();
  }

  public SourceType getSourceTypeOfParent() {
    return getEp1SourceType();
  }

  public SourceType getSourceTypeOfChildren() {
    return getEp2SourceType();
  }
}
