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
import com.cloudera.nav.plugin.model.entities.Entity;
import com.cloudera.nav.plugin.model.relations.DataFlowRelation;
import com.cloudera.nav.plugin.model.relations.InstanceOfRelation;
import com.cloudera.nav.plugin.model.relations.LogicalPhysicalRelation;
import com.cloudera.nav.plugin.model.relations.ParentChildRelation;
import com.cloudera.nav.plugin.model.relations.Relation;
import com.cloudera.nav.plugin.model.relations.RelationIdGenerator;
import com.cloudera.nav.plugin.model.relations.RelationRole;
import com.cloudera.nav.plugin.model.relations.RelationType;

import java.util.Collection;

/**
 * Creates Relation instances from the information in MRelationEntry
 */
public class RelationFactory {

  public Relation createRelation(RelationType type, Entity entity,
                                 Object other, RelationRole roleOfOther,
                                 SourceType sourceTypeOfOther,
                                 String namespace) {
    switch(type) {
      case DATA_FLOW:
        return createDataFlowRelation(roleOfOther, sourceTypeOfOther, entity,
            other, namespace);
      case PARENT_CHILD:
        return createParentChildRelation(roleOfOther, sourceTypeOfOther,
            entity, other, namespace);
      case LOGICAL_PHYSICAL:
        return createLogicalPhysicalRelation(roleOfOther, sourceTypeOfOther,
            entity, other, namespace);
      case INSTANCE_OF:
        return createInstanceOfRelation(roleOfOther, sourceTypeOfOther, entity,
            other, namespace);
      default:
        throw new IllegalArgumentException("Invalid RelationType " +
            type.toString());
    }
  }

  @SuppressWarnings("unchecked")
  private Relation createDataFlowRelation(RelationRole roleOfOther,
                                          SourceType sourceTypeOfOther,
                                          Entity entity, Object other,
                                          String namespace) {
    DataFlowRelation.Builder builder = DataFlowRelation.builder();
    if (roleOfOther == RelationRole.SOURCE) {
      builder.target(entity);
      if (Entity.class.isAssignableFrom(other.getClass())) {
        builder.source((Entity)other);
      } else if (other instanceof String) {
        builder.sourceId((String)other).sourceTypeOfSource(sourceTypeOfOther);
      } else {
        Class<?> typeClass = other.getClass().getTypeParameters()[0]
            .getClass();
        if (Entity.class.isAssignableFrom(typeClass)) {
          builder.sources((Collection<Entity>) other);
        } else if (typeClass == String.class) {
          builder.sourceIds((Collection<String>) other)
              .sourceTypeOfSource(sourceTypeOfOther);
        }
      }
    } else {
      builder.source(entity);
      if (Entity.class.isAssignableFrom(other.getClass())) {
        builder.target((Entity)other);
      } else if (other instanceof String) {
        builder.targetId((String)other).sourceTypeOfTarget(sourceTypeOfOther);
      } else {
        Class<?> typeClass = other.getClass().getTypeParameters()[0]
            .getClass();
        if (Entity.class.isAssignableFrom(typeClass)) {
          builder.targets((Collection<Entity>) other);
        } else if (typeClass == String.class) {
          builder.targetIds((Collection<String>) other)
              .sourceTypeOfTarget(sourceTypeOfOther);
        }
      }
    }
    return builder.idGenerator(new RelationIdGenerator())
        .namespace(namespace).build();
  }

  @SuppressWarnings("unchecked")
  private Relation createParentChildRelation(RelationRole roleOfOther,
                                             SourceType sourceTypeOfOther,
                                             Entity entity, Object other,
                                             String namespace) {
    ParentChildRelation.Builder builder = ParentChildRelation.builder();
    if (roleOfOther == RelationRole.PARENT) {
      builder.child(entity);
      if (Entity.class.isAssignableFrom(other.getClass())) {
        builder.parent((Entity) other);
      } else if (other instanceof String) {
        builder.parentId((String) other).sourceTypeOfParent(sourceTypeOfOther);
      } else {
        throw new IllegalArgumentException("Only one parent allowed");
      }
    } else {
      builder.parent(entity);
      if (Entity.class.isAssignableFrom(other.getClass())) {
        builder.child((Entity) other);
      } else if (other instanceof String) {
        builder.childId((String) other).sourceTypeOfChildren(sourceTypeOfOther);
      } else {
        Class<?> typeClass = other.getClass().getTypeParameters()[0]
            .getClass();
        if (Entity.class.isAssignableFrom(typeClass)) {
          builder.children((Collection<Entity>) other);
        } else if (typeClass == String.class) {
          builder.childrenIds((Collection<String>) other)
              .sourceTypeOfChildren(sourceTypeOfOther);
        }
      }
    }
    return builder.idGenerator(new RelationIdGenerator())
        .namespace(namespace).build();
  }

  @SuppressWarnings("unchecked")
  private Relation createInstanceOfRelation(RelationRole roleOfOther,
                                            SourceType sourceTypeOfOther,
                                            Entity entity, Object other,
                                            String namespace) {
    InstanceOfRelation.Builder builder = InstanceOfRelation.builder();
    if (roleOfOther == RelationRole.TEMPLATE) {
      builder.instance(entity);
      if (Entity.class.isAssignableFrom(other.getClass())) {
        builder.template((Entity) other);
      } else if (other instanceof String) {
        builder.templateId((String) other)
            .sourceTypeOfTemplate(sourceTypeOfOther);
      } else {
        throw new IllegalArgumentException("Only one template allowed");
      }
    } else {
      builder.template(entity);
      if (Entity.class.isAssignableFrom(other.getClass())) {
        builder.instance((Entity) other);
      } else if (other instanceof String) {
        builder.instanceId((String) other)
            .sourceTypeOfInstance(sourceTypeOfOther);
      } else {
        throw new IllegalArgumentException("Only one instance allowed");
      }
    }
    return builder.idGenerator(new RelationIdGenerator())
        .namespace(namespace).build();
  }

  @SuppressWarnings("unchecked")
  private Relation createLogicalPhysicalRelation(RelationRole roleOfOther,
                                                 SourceType sourceTypeOfOther,
                                                 Entity entity, Object other,
                                                 String namespace) {
    LogicalPhysicalRelation.Builder builder = LogicalPhysicalRelation.builder();
    if (roleOfOther == RelationRole.LOGICAL) {
      builder.physical(entity);
      if (Entity.class.isAssignableFrom(other.getClass())) {
        builder.logical((Entity) other);
      } else if (other instanceof String) {
        builder.logicalId((String) other).sourceTypeOfLogical(sourceTypeOfOther);
      } else {
        throw new IllegalArgumentException("Only one logical allowed");
      }
    } else {
      builder.logical(entity);
      if (Entity.class.isAssignableFrom(other.getClass())) {
        builder.physical((Entity) other);
      } else if (other instanceof String) {
        builder.physicalId((String) other)
            .sourceTypeOfPhysical(sourceTypeOfOther);
      } else {
        Class<?> typeClass = other.getClass().getTypeParameters()[0]
            .getClass();
        if (Entity.class.isAssignableFrom(typeClass)) {
          builder.physical((Collection<Entity>) other);
        } else if (typeClass == String.class) {
          builder.physicalIds((Collection<String>) other)
              .sourceTypeOfPhysical(sourceTypeOfOther);
        }
      }
    }
    return builder.idGenerator(new RelationIdGenerator())
        .namespace(namespace).build();
  }

}
