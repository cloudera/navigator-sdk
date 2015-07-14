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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

/**
 * Add, remove, override user-defined properties
 */
public class UDPChangeSet {

  @JsonProperty("add")
  private Map<String, String> newProperties;
  @JsonProperty("del")
  private Set<String> removeProperties;
  @JsonProperty("set")
  private Map<String, String> overrideProperties;

  public UDPChangeSet() {
    newProperties = Maps.newHashMap();
    removeProperties = Sets.newHashSet();
    overrideProperties = null;
  }

  public void reset() {
    newProperties.clear();
    removeProperties.clear();
    overrideProperties = null;
  }

  /**
   * Add specified properties to properties to be added
   * @param properties
   */
  public void addProperties(Map<String, String> properties) {
    if (MapUtils.isNotEmpty(properties)) {
      newProperties.putAll(properties);
      removeProperties.removeAll(properties.keySet());
      if (overrideProperties != null) {
        overrideProperties = Maps.newHashMap(Maps.difference(overrideProperties,
            properties).entriesOnlyOnLeft());
      }
    }
  }

  /**
   * Add specified properties to properties to be removed
   * @param keys
   */
  public void removeProperties(Collection<String> keys) {
    if (CollectionUtils.isNotEmpty(keys)) {
      removeProperties.addAll(keys);
      for (String k : keys) {
        newProperties.remove(k);
      }
      if (overrideProperties != null) {
        for (String k : keys) {
          overrideProperties.remove(k);
        }
      }
    }
  }

  /**
   * Replace existing properties with specified properties
   * @param properties
   */
  public void setProperties(Map<String, String> properties) {
    if (properties == null) {
      overrideProperties = null;
    } else {
      removeProperties.removeAll(properties.keySet());
      newProperties = Maps.newHashMap(Maps.difference(newProperties, properties)
          .entriesOnlyOnLeft());
      overrideProperties = Maps.newHashMap(properties);
    }
  }

  public Map<String, String> getNewProperties() {
    return newProperties;
  }

  public void setNewProperties(Map<String, String> newProperties) {
    this.newProperties = newProperties;
  }

  public Map<String, String> getOverrideProperties() {
    return overrideProperties;
  }

  public void setOverrideProperties(Map<String, String> overrideProperties) {
    this.overrideProperties = overrideProperties;
  }

  public Set<String> getRemoveProperties() {
    return removeProperties;
  }

  public void setRemoveProperties(Set<String> removeProperties) {
    this.removeProperties = removeProperties;
  }
}
