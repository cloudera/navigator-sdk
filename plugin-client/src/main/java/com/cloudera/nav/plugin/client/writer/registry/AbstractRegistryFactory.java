// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.client.writer.registry;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collection;

public abstract class AbstractRegistryFactory<T> {

  private static final int MAX_CACHE_SIZE = 100;

  public LoadingCache<Class<?>, Collection<T>> newRegistry() {
    return CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE)
        .build(new CacheLoader<Class<?>, Collection<T>>() {
          @Override
          public Collection<T> load(Class<?> aClass) throws Exception {
            return parseMClass(aClass);
          }
        });
  }

  private Collection<T> parseMClass(Class<?> aClass) {
    Collection<T> properties = Lists.newArrayList();
    for (Method method : aClass.getMethods()) {
      if (isValidMethod(method)) {
        properties.add(createEntry(method));
      }
    }
    return properties;
  }

  private boolean isValidMethod(Method method) {
    if (method.isAnnotationPresent(getTypeClass()) &&
        !method.isBridge()) {
      // TODO can we validate earlier?
      Preconditions.checkArgument(method.getParameterTypes().length == 0,
          "MProperty should only be used on getter methods");
      return true;
    }
    return false;
  }

  protected abstract Class<? extends Annotation> getTypeClass();

  protected abstract T createEntry(Method method);
}
