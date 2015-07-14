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

import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.google.common.base.Throwables;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.commons.lang.StringUtils;

/**
 * Registry entry for a particular instance of the @MProperty annotation
 */
public class MPropertyEntry {

  private final String attribute;
  private final Field field;
  private final Method getter;
  private final MProperty ann;

  public MPropertyEntry(Field field, Method getter) {
    ann = field.getAnnotation(MProperty.class);
    String attr = ann.attribute();
    this.attribute = StringUtils.isEmpty(attr) ? field.getName() : attr;
    this.field = field;
    this.getter = getter;
  }

  /**
   * @return Name of the metadata field
   */
  public String getAttribute() {
    return attribute;
  }

  /**
   * @return the Field that is associated with the @MProperty annotation
   */
  public Field getField() {
    return field;
  }

  /**
   * @return Getter method that takes no parameters and return the value
   */
  public Method getReadMethod() {
    return getter;
  }

  /**
   * @return the @MProperty annotation associated with this field
   */
  public MProperty getAnnotation() {
    return ann;
  }

  /**
   * Get the attribute field value
   *
   * @param mClassObj
   * @return
   */
  public Object getValue(Object mClassObj) {
    try {
      return getter.invoke(mClassObj);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @return whether this is a required property
   */
  public boolean required() {
    return ann.required();
  }
}
