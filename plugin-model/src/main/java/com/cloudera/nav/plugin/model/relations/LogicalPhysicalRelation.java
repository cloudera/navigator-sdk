// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.model.relations;

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.entities.Entity;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.Collections;

/**
 * 1-to-many relationship between a logical entity and its physical
 * manifestations (e.g., Hive table and HDFS directory/files)
 */
@MClass
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
