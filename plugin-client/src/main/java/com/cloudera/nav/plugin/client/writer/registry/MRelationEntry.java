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

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MRelation;
import com.cloudera.nav.plugin.model.entities.Entity;
import com.cloudera.nav.plugin.model.relations.Relation;
import com.cloudera.nav.plugin.model.relations.RelationRole;
import com.cloudera.nav.plugin.model.relations.RelationType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;

/**
 * Represents a single relationship declared in a Entity's MRelation annotated
 * getter method
 */
public class MRelationEntry {

  private final Method method;
  private final MRelation relationAnn;
  private final Map<RelationRole, RelationType> roleToTypeMap;

  public MRelationEntry(Method method) {
    validateReturnType(method);
    this.method = method;
    this.relationAnn = method.getAnnotation(MRelation.class);
    SourceType sourceTypeOfOther = relationAnn.sourceType();
    Preconditions.checkArgument(sourceTypeOfOther != null ||
        isConnectedToEntity(), "Must supply source type if relation is " +
            "connected to string entity id.");
    roleToTypeMap = Maps.newHashMap();
  }

  /**
   * Build Relation based on the entity and the MRelation annotation information
   * Note that this MRelationEntry's associated method must exist in the
   * entity's class
   *
   * @param entity
   * @return
   */
  public Relation buildRelation(Entity entity, String namespace) {
    RelationRole roleOfOther = relationAnn.role();
    RelationType type = getRelationTypeFromRole(roleOfOther);
    SourceType sourceTypeOfOther = relationAnn.sourceType();
    Object other = getValue(entity);
    RelationFactory factory = new RelationFactory();
    return factory.createRelation(type, entity, other, roleOfOther,
        sourceTypeOfOther, namespace);
  }

  private RelationType getRelationTypeFromRole(RelationRole roleOfOther) {
    if (roleToTypeMap.size() == 0) {
      for (RelationType type : RelationType.values()) {
        roleToTypeMap.put(type.getEndpoint1Role(), type);
        roleToTypeMap.put(type.getEndpoint2Role(), type);
      }
    }
    return roleToTypeMap.get(roleOfOther);
  }

  /**
   * @return whether the MRelation is connected to just a string id or
   * another Entity with MClass annotations
   */
  public boolean isConnectedToEntity() {
    Class<?> aClass = method.getReturnType();
    return Entity.class.isAssignableFrom(aClass) ||
        (Collection.class.isAssignableFrom(aClass) &&
            Entity.class.isAssignableFrom(getTypeParameterClass(method)));
  }

  /**
   * @return collection of entities connected to the given entity through the
   * relationship represented by this MRelationEntry
   */
  @SuppressWarnings("unchecked")
  public Collection<? extends Entity> getConnectedEntities(Entity entity) {
    Preconditions.checkArgument(isConnectedToEntity());
    if (Entity.class.isAssignableFrom(method.getReturnType())) {
      return ImmutableList.of((Entity)getValue(entity));
    } else {
      return (Collection<Entity>)getValue(entity);
    }
  }

  private Object getValue(Entity entity) {
    try {
      return method.invoke(entity);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      throw Throwables.propagate(e);
    }
  }

  private void validateReturnType(Method method) {
    // String, Entity, Collection<String>, Collection<Entity>
    Class<?> aClass = method.getReturnType();
    if (!isStringOrEntity(aClass)) {
      Preconditions.checkArgument(Collection.class.isAssignableFrom(aClass));
      Class<?> typeClass = getTypeParameterClass(method);
      Preconditions.checkArgument(isStringOrEntity(typeClass));
    }
  }

  private Class<?> getTypeParameterClass(Method method) {
    ParameterizedType type = (ParameterizedType) method.getGenericReturnType();
    Type[] typeParams = type.getActualTypeArguments();
    Preconditions.checkArgument(typeParams.length == 1);
    return (Class<?>)typeParams[0];
  }

  private boolean isStringOrEntity(Class<?> aClass) {
    return aClass == String.class ||
        Entity.class.isAssignableFrom(aClass);
  }

}
