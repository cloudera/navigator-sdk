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
import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;

/**
 * Custom metadata model which belongs to either the default Navigator package
 * or to a user-defined custom package. The pair of packageName and name must
 * be unique for a MetaClass
 *
 * New instances should be created via the newClass static factory method so
 * that we can make it harder to miss required properties at compile time
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetaClass extends BaseModelObject {

  public static MetaClass newClass(String packageName, String name) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(packageName));
    Preconditions.checkArgument(StringUtils.isNotEmpty(name));
    MetaClass metaClass = new MetaClass();
    metaClass.setPackageName(packageName);
    metaClass.setName(name);
    return metaClass;
  }

  private String packageName;

  /**
   * The namespace for this model
   */
  public String getPackageName() {
    return packageName;
  }

  public void setPackageName(String packageName) {
    this.packageName = packageName;
  }

  @Override
  public int hashCode() {
    return getNameHashCode() * 31 + getPackageNameHashCode();
  }

  private int getNameHashCode() {
    return getName() == null ? "".hashCode() : getName().hashCode();
  }

  private int getPackageNameHashCode() {
    return getPackageName() == null ? "".hashCode() :
        getPackageName().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof MetaClass) {
      MetaClass o = (MetaClass) other;
      return StringUtils.equals(getName(), o.getName()) &&
          StringUtils.equals(getPackageName(), o.getPackageName());
    }
    return false;
  }
}