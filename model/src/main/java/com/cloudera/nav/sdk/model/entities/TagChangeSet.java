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

package com.cloudera.nav.sdk.model.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

/**
 * Add, remove, and override tags
 */
public class TagChangeSet {

  @JsonProperty("add")
  private Set<String> newTags;
  @JsonProperty("del")
  private Set<String> delTags;
  @JsonProperty("set")
  private Set<String> overrideTags;

  public TagChangeSet() {
    newTags = Sets.newHashSet();
    delTags = Sets.newHashSet();
    overrideTags = null;
  }

  /**
   * Add specified tags to the set of tags to be appended
   * @param tags
   */
  public void appendTags(Collection<String> tags) {
    if (CollectionUtils.isNotEmpty(tags)) {
      newTags.addAll(tags);
      delTags.removeAll(tags);
      if (overrideTags != null) {
        overrideTags.removeAll(tags);
      }
    }
  }

  /**
   * Add the specified tags to the set of tags to be removed
   * @param tags
   */
  public void removeTags(Collection<String> tags) {
    if (CollectionUtils.isNotEmpty(tags)) {
      delTags.addAll(tags);
      newTags.removeAll(tags);
      if (overrideTags != null) {
        overrideTags.removeAll(tags);
      }
    }
  }

  /**
   * Replace existing tags with specified tags
   * @param tags if null then no overriding, if empty set then remove all tags
   */
  public void setTags(Collection<String> tags) {
    if (tags == null) {
      overrideTags = null;
    } else {
      overrideTags = Sets.newHashSet(tags);
      delTags.removeAll(tags);
      newTags.removeAll(tags);
    }
  }

  /**
   * Clear this change set
   */
  public void reset() {
    newTags.clear();
    delTags.clear();
    overrideTags = null;
  }

  public Set<String> getNewTags() {
    return newTags;
  }

  public void setNewTags(Set<String> newTags) {
    if (newTags == null) {
      newTags = Sets.newHashSet();
    }
    this.newTags = newTags;
  }
  /**
   * Set of tags to replace existing tags. Null means no replacement,
   * empty set means remove all existing tags
   */
  public Set<String> getOverrideTags() {
    return overrideTags;
  }

  public boolean hasOverrides() {
    return overrideTags != null;
  }

  public void setOverrideTags(Set<String> overrideTags) {
    this.overrideTags = overrideTags;
  }

  public Set<String> getDelTags() {
    return delTags;
  }

  public void setDelTags(Set<String> delTags) {
    if (delTags == null) {
      delTags = Sets.newHashSet();
    }
    this.delTags = delTags;
  }
}
