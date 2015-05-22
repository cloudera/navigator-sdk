// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.client.writer.registry;

import com.google.common.base.Throwables;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Registry entry for MProperty used by the MClassRegistry
 */
public class MPropertyEntry {

  private final String attribute;
  private final Method method;

  public MPropertyEntry(String attribute, Method method) {
    this.attribute = attribute;
    this.method = method;
  }

  /**
   * @return Name of the metadata field
   */
  public String getAttribute() {
    return attribute;
  }

  /**
   * @return Getter method that takes no parameters and return the value
   */
  public Method getMethod() {
    return method;
  }

  /**
   * Get the attribute field value
   *
   * @param mClassObj
   * @return
   */
  public Object getValue(Object mClassObj) {
    try {
      return method.invoke(mClassObj);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
  }
}
