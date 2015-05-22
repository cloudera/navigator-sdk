// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.client.writer.registry;

import com.cloudera.nav.plugin.model.annotations.MRelation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class MRelationRegistryFactory extends
    AbstractRegistryFactory<MRelationEntry> {

  @Override
  protected Class<? extends Annotation> getTypeClass() {
    return MRelation.class;
  }

  @Override
  protected MRelationEntry createEntry(Method method) {
    return new MRelationEntry(method);
  }

}
