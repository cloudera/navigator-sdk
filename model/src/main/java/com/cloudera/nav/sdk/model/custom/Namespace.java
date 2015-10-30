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
 * Defines groups of custom metadata properties
 *
 * New instances should be created via the newNamespace static factory method so
 * that we can make it harder to miss required properties at compile time
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Namespace extends BaseModelObject {

  public static final String DEFAULT_NAMESPACE_NAME = "nav";

  public static Namespace newNamespace(String name) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(name));
    Namespace ns = new Namespace();
    ns.setName(name);
    return ns;
  }

  private boolean external;

  public boolean isDefaultNamespace() {
    return StringUtils.equals(DEFAULT_NAMESPACE_NAME, getName());
  }

  /**
   * Whether this namespace is for an external third-party application
   */
  public boolean isExternal() {
    return external;
  }

  public void setExternal(boolean external) {
    this.external = external;
  }

  @Override
  public int hashCode() {
    return getName() == null ? "".hashCode() : getName().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return other != null &&
        other instanceof Namespace &&
        getName() != null && getName().equals(
        ((Namespace) other).getName());
  }
}
