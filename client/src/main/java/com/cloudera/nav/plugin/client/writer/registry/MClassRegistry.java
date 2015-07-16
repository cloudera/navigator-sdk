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
package com.cloudera.nav.plugin.client.writer.registry;


import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.entities.Entity;
import com.cloudera.nav.plugin.model.relations.Relation;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

/**
 * Used by MetadataWriter's to remember the MProperty and MRelations
 */
public class MClassRegistry {

  private final LoadingCache<Class<?>, Collection<MPropertyEntry>>
      propertyRegistry;
  private final LoadingCache<Class<?>, Collection<MRelationEntry>>
      relationRegistry;

  public MClassRegistry() {
    propertyRegistry = (new MPropertyEntryFactory()).newRegistry();
    relationRegistry = (new MRelationEntryFactory()).newRegistry();
  }

  /**
   * Get the @MProperty annotation information from a given class
   * @param aClass
   * @return
   */
  public Collection<MPropertyEntry> getProperties(Class<?> aClass) {
    Preconditions.checkArgument(Relation.class.isAssignableFrom(aClass) ||
        aClass.isAnnotationPresent(MClass.class));
    try {
      return propertyRegistry.get(aClass);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Get the @MRelation annotation information from a given Entity subclass
   * @param aClass
   * @return
   */
  public Collection<MRelationEntry> getRelations(
      Class<? extends Entity> aClass) {
    try {
      return relationRegistry.get(aClass);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  public void reset() {
    propertyRegistry.invalidateAll();
    relationRegistry.invalidateAll();
  }

  public void validateRequiredMProperties(Entity en) {
    Object value;
    boolean b;
    for (MPropertyEntry prop : getProperties(en.getClass())) {
      if (prop.required()) {
        value = prop.getValue(en);
        if (value instanceof String) {
          b = StringUtils.isNotEmpty((String)value);
        } else if (value instanceof Collection) {
          b = CollectionUtils.isNotEmpty((Collection)value);
        } else {
          b = value != null;
        }
        Preconditions.checkArgument(b, String.format("Value of required " +
            "property %s null or empty", prop.getField().getName()));
      }
    }
    for (MRelationEntry prop : getRelations(en.getClass())) {
      if (prop.required()) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(prop
                .getConnectedEntities(en)),
            String.format("Value of required relation %s null or empty",
                prop.getName()));
      }
    }
  }
}
