// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
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
