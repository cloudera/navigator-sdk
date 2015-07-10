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

import com.cloudera.nav.plugin.model.MD5IdGenerator;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Add, remove, override properties
 */
public class UDPChangeSet {

  public static final String WILDCARD = MD5IdGenerator.generateIdentity("*");
  private final Map<String, String> newProperties;
  private final Set<String> removeProperties;

  public UDPChangeSet() {
    newProperties = Maps.newHashMap();
    removeProperties = Sets.newHashSet();
  }

  public void clear() {
    newProperties.clear();
    removeProperties.clear();
    removeProperties.add(WILDCARD);
  }

  public void addAll(Map<String, String> properties) {
    removeProperties.removeAll(properties.keySet());
    newProperties.putAll(properties);
  }

  public void removeAll(Collection<String> keys) {
    for (String k : keys) {
      newProperties.remove(k);
    }
    removeProperties.addAll(keys);
  }

  public Map<String, String> getNewProperties() {
    return newProperties;
  }

  public Set<String> getRemoveProperties() {
    return removeProperties;
  }
}
