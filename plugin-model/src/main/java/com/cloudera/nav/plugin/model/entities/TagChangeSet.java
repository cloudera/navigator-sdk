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
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;

/**
 * Add, remove, and override tags
 */
public class TagChangeSet {

  public static final String WILDCARD = MD5IdGenerator.generateIdentity("*");
  private final Set<String> newTags;
  private final Set<String> delTags;

  public TagChangeSet() {
    newTags = Sets.newHashSet();
    delTags = Sets.newHashSet();
  }

  /**
   * Append given tags
   * @param tags
   */
  public void addTags(Collection<String> tags) {
    delTags.removeAll(tags);
    newTags.addAll(tags);
  }

  /**
   * Remove given tags
   * @param tags
   */
  public void removeAll(Collection<String> tags) {
    newTags.removeAll(tags);
    delTags.addAll(tags);
  }

  public void clear() {
    newTags.clear();
    delTags.clear();
    delTags.add(WILDCARD);
  }

  public Set<String> getNewTags() {
    return newTags;
  }

  public Set<String> getDelTags() {
    return delTags;
  }
}
