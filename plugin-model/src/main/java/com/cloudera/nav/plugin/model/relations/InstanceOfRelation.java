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
 * Represents the relationship between an operation template and execution
 */
@MClass
public final class InstanceOfRelation extends Relation {

  public static class Builder<T extends Builder<T>> extends Relation.Builder<T> {

    protected Builder() {
      super(RelationType.INSTANCE_OF);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected T self() {
      return (T) this;
    }

    @Override
    public InstanceOfRelation build() {
      return new InstanceOfRelation(this);
    }

    public T templateId(String templateId) {
      return ep1Ids(Collections.singleton(templateId));
    }

    public T instanceId(String instanceId) {
      return ep2Ids(Collections.singleton(instanceId));
    }

    public T sourceTypeOfTemplate(SourceType sourceType) {
      return ep1SourceType(sourceType);
    }

    public T sourceTypeOfInstance(SourceType sourceType) {
      return ep2SourceType(sourceType);
    }

    public T template(Entity src) {
      return ep1(ImmutableList.of(src));
    }

    public T instance(Entity instance) {
      return instances(Collections.singleton(instance));
    }

    public T instances(Collection<Entity> instance) {
      return ep2(instance);
    }
  }

  @SuppressWarnings("rawtypes")
  public static Builder<?> builder() {
    return new Builder();
  }

  private InstanceOfRelation(Builder<?> builder) {
    super(builder);
  }

  public String getTemplateId() {
    return Iterables.getOnlyElement(getEp1Ids());
  }

  public Collection<String> getInstanceIds() {
    return getEp2Ids();
  }

  public SourceType getSourceTypeOfTemplate() {
    return getEp1SourceType();
  }

  public SourceType getSourceTypeOfInstance() {
    return getEp2SourceType();
  }
}
