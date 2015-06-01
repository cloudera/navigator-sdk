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

import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.google.common.base.Preconditions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

public class ValidationUtil {

  public void validateRequiredMProperties(Object mPropertyObj) {
    for (Method method : mPropertyObj.getClass().getMethods()) {
      if (!method.isBridge() && method.isAnnotationPresent(MProperty.class)
          && method.getAnnotation(MProperty.class).required()) {
        Class<?> returnType = method.getReturnType();
        Object value;
        try {
          value = method.invoke(mPropertyObj);
        } catch (IllegalAccessException e) {
          throw new IllegalArgumentException(
              "Could not access MProperty method " + method.getName(), e);
        } catch (InvocationTargetException e) {
          throw new IllegalArgumentException(
              "Could not get MProperty value for " + method.getName(), e);
        }
        if (Collection.class.isAssignableFrom(returnType)) {
          Preconditions.checkArgument(!CollectionUtils.isEmpty(
              (Collection) value), method.getName() +
              " returned empty collection");
        } else if (String.class.isAssignableFrom(returnType)) {
          Preconditions.checkArgument(!StringUtils.isEmpty(
              (String)value), method.getName() +
              " returned null or empty string");
        } else {
          Preconditions.checkArgument(value != null,
              method.getName() + " returned null");
        }
      }
    }
  }
}
