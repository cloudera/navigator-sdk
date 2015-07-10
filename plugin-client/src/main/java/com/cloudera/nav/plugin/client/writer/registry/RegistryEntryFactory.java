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
package com.cloudera.nav.plugin.client.writer.registry;

import com.cloudera.nav.plugin.model.MClassUtil;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

import java.beans.IntrospectionException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;

/**
 * Abstract factory for creating a new registry of MClass objects
 * @param <T> the type of registry entries
 */
public abstract class RegistryEntryFactory<T> {

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

  private Collection<T> parseMClass(Class<?> aClass)
      throws IntrospectionException {
    Map<Field, Method> annInfo = MClassUtil.getAnnotatedProperties(aClass,
        getTypeClass());
    Collection<T> properties = Lists.newLinkedList();
    for (Map.Entry<Field, Method> entry : annInfo.entrySet()) {
      properties.add(createEntry(entry.getKey(), entry.getValue()));
    }
    return properties;
  }

  protected abstract Class<? extends Annotation> getTypeClass();

  protected abstract T createEntry(Field field, Method getter);
}
