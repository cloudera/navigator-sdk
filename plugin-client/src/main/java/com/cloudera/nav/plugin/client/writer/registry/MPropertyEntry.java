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
