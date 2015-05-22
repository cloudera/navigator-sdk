// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.client.writer.registry;


import com.cloudera.nav.plugin.model.entities.Entity;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

/**
 * Used by MetadataWriter's to remember the MProperty and MRelations
 */
public class MClassRegistry {

  private final LoadingCache<Class<?>, Collection<MPropertyEntry>>
      propertyRegistry;
  private final LoadingCache<Class<?>, Collection<MRelationEntry>>
      relationRegistry;

  public MClassRegistry() {
    propertyRegistry = (new MPropertyRegistryFactory()).newRegistry();
    relationRegistry = (new MRelationRegistryFactory()).newRegistry();
  }

  public Collection<MPropertyEntry> getProperties(Class<?> aClass) {
    Preconditions.checkArgument(aClass.isAnnotationPresent(MClass.class));
    try {
      return propertyRegistry.get(aClass);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  public Collection<MRelationEntry> getRelations(
      Class<? extends Entity> aClass) {
    try {
      return relationRegistry.get(aClass);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  public void reset() {
    propertyRegistry.invalidateAll();
    relationRegistry.invalidateAll();
  }
}
