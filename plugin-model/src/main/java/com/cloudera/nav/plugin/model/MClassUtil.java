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

package com.cloudera.nav.plugin.model;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;

/**
 * Utilities for retrieving @MProperty and @MRelation fields
 * and matching them with getters
 */
public class MClassUtil {

  /**
   * @param mclass the metadata object class
   * @param annClass the annotation class
   * @return annotated fields and corresponding getter
   */
  public static Map<Field, Method> getAnnotatedProperties(
      Class<?> mclass, Class<? extends Annotation> annClass) {
    Map<String, Method> getters = Maps.newHashMap();
    try {
      for (PropertyDescriptor pd : Introspector.getBeanInfo(mclass)
          .getPropertyDescriptors()) {
        getters.put(pd.getName(), pd.getReadMethod());
      }
    } catch(IntrospectionException e) {
      throw Throwables.propagate(e);
    }

    Map<Field, Method> properties = Maps.newHashMap();
    for (Field field : getValidFields(mclass, annClass)) {
      Preconditions.checkArgument(getters.containsKey(field.getName()),
          "No getter method found for " + field.getName());
      properties.put(field, getters.get(field.getName()));
    }
    return properties;
  }

  private static Collection<Field> getValidFields(
      Class<?> mclass, Class<? extends Annotation> annClass) {
    Collection<Field> fields = Lists.newLinkedList();
    for (Field field : mclass.getDeclaredFields()) {
      if (!field.isSynthetic() && field.isAnnotationPresent(annClass)) {
        fields.add(field);
      }
    }
    Class<?> superClass = mclass.getSuperclass();
    if (superClass != null) {
      fields.addAll(getValidFields(superClass, annClass));
    }
    return fields;
  }
}
