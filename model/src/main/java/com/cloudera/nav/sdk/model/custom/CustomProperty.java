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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * A managed custom property which has a type and belong to a namespace and
 * associated with metadata classes.
 * Optionally a managed user-defined property can have validation
 * constraints (max length and a regex pattern).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CustomProperty extends BaseModelObject {

  public static CustomProperty newProperty(String namespace,
                                           String name,
                                           CustomPropertyType propertyType,
                                           boolean multiValued, String[] values) {
    CustomProperty prop = new CustomProperty();
    prop.setNamespace(namespace);
    prop.setName(name);
    prop.setPropertyType(propertyType);
    prop.setMultiValued(multiValued);
    if (propertyType == CustomPropertyType.ENUM) {
      Preconditions.checkNotNull(values);
      Preconditions.checkArgument(values.length > 0);
      prop.setEnumValues(Sets.newHashSet(values));
    }
    return prop;
  }

  private String namespace;
  @JsonProperty("type")
  private CustomPropertyType propertyType;
  private boolean multiValued;
  private Integer maxLength;
  private String pattern;
  private Set<String> enumValues;

  /**
   * Type of property values
   */
  public CustomPropertyType getPropertyType() {
    return propertyType;
  }

  public void setPropertyType(CustomPropertyType propertyType) {
    this.propertyType = propertyType;
  }

  /**
   * Whether the property can contain multiple values
   */
  public boolean isMultiValued() {
    return multiValued;
  }

  public void setMultiValued(boolean multiValued) {
    this.multiValued = multiValued;
  }

  /**
   * For String/Text properties only, the maximum number of characters for
   * property values
   */
  public Integer getMaxLength() {
    return maxLength;
  }

  public void setMaxLength(Integer maxLength) {
    this.maxLength = maxLength;
  }

  /**
   * For String/Text properties only, a regex pattern that property values must
   * match
   */
  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  /**
   * The namespace that this property belongs to
   */
  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  /**
   * Enum constants for enum properties
   */
  public Set<String> getEnumValues() {
    return enumValues;
  }

  public void setEnumValues(Set<String> enumValues) {
    this.enumValues = enumValues;
  }

  @Override
  public int hashCode() {
    return 31 * getNameHashCode() + getNamespaceHashCode();
  }

  private int getNameHashCode() {
    return getName() == null ? "".hashCode() : getName().hashCode();
  }

  private int getNamespaceHashCode() {
    return getNamespace() == null ? "".hashCode() : getNamespace().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof CustomProperty) {
      CustomProperty o = (CustomProperty) other;
      return StringUtils.equals(getName(), o.getName()) &&
          StringUtils.equals(getNamespace(), o.getNamespace());
    }
    return false;
  }
}
