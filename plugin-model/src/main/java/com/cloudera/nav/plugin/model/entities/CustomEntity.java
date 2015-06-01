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
package com.cloudera.nav.plugin.model.entities;

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MProperty;

/**
 * Abstract base classes for creating custom entities defined by non-Hadoop
 * applications
 */
public abstract class CustomEntity extends Entity {

  private String namespace;

  /**
   * @return Navigator assigned namespace for the custom entity
   */
  @MProperty(required=true)
  public String getNamespace() {
    return namespace;
  }

  @Override
  @MProperty(required=true)
  public SourceType getSourceType() {
    return SourceType.PLUGIN;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }
}
