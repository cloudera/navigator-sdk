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
package com.cloudera.nav.sdk.model.annotations;

import com.cloudera.nav.sdk.model.custom.CustomPropertyType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Identifies a getter method as an index property.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface MProperty {

  /**
   * Override for the property name
   */
  String attribute() default "";

  /**
   * Client plugin will throw exception on write if value of property was null
   */
  boolean required() default false;

  /**
   * If false then this property is treated as a dynamic text field. Currently
   * this is opt-in because the custom metadata features in Navigator 2.6 are
   * still experimental
   */
  boolean register() default false;

  /**
   * The field type for registered fields
   */
  CustomPropertyType fieldType() default CustomPropertyType.TEXT;

  /**
   * Validation for field values (server-side) if fieldType == TEXT
   */
  String pattern() default "";

  /**
   * Validation for field values (server-side) if fieldType == TEXT
   * Default max length of 0 means no validation
   */
  int maxLength() default 0;

  /**
   * Allowed values if fieldType == ENUM
   */
  String[] values() default "";
}
