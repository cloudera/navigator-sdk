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
package com.cloudera.nav.sdk.model.custom;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Type info for the field. There is a known bug on the server side preventing
 * successful processing of DATE fields so that's been disabled for now.
 */
public enum CustomPropertyType {
  BOOLEAN(Boolean.class,
      Sets.<Class<?>>newHashSet(Boolean.class, boolean.class)),
  DOUBLE(Double.class,
      Sets.<Class<?>>newHashSet(Double.class, double.class,
          Float.class, float.class)),
  FLOAT(Float.class,
      Sets.<Class<?>>newHashSet(Float.class, float.class, double.class,
          Double.class)),
  INTEGER(Integer.class,
      Sets.<Class<?>>newHashSet(Integer.class, int.class)),
  LONG(Long.class,
      Sets.<Class<?>>newHashSet(Long.class, long.class, Integer.class,
          int.class)),
  TEXT(String.class, Sets.<Class<?>>newHashSet(String.class)),
  ENUM(Enum.class, Sets.<Class<?>>newHashSet(String.class, Enum.class));

  private final Class<?> valueType;
  private final Set<Class<?>> validInputTypes;

  /**
   * @param valueType
   */
  private CustomPropertyType(Class<?> valueType,
                             Set<Class<?>> validInputTypes) {
    Preconditions.checkArgument(validInputTypes.contains(valueType));
    this.valueType = valueType;
    this.validInputTypes = validInputTypes;
  }

  public Class<?> getValueType() {
    return valueType;
  }

  public Set<Class<?>> getValidInputTypes() {
    return validInputTypes;
  }
}
