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
package com.cloudera.nav.plugin.client.writer;

import com.cloudera.nav.plugin.client.PluginConfigurations;
import com.cloudera.nav.plugin.client.writer.registry.MClassRegistry;
import com.cloudera.nav.plugin.client.writer.registry.MPropertyEntry;
import com.cloudera.nav.plugin.client.writer.registry.MRelationEntry;
import com.cloudera.nav.plugin.model.MD5IdGenerator;
import com.cloudera.nav.plugin.model.ValidationUtil;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.entities.CustomEntity;
import com.cloudera.nav.plugin.model.entities.Entity;
import com.cloudera.nav.plugin.model.relations.Relation;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

/**
 * Responsible for writing a given metadata entity and implied relations
 * to the underlying storage. Currently supports HDFS and local file system
 */
public abstract class MetadataWriter {

  // attribute name for type of metadata object (ENTITY or RELATION)
  public static final String MTYPE = "__MTYPE__";
  // attribute name for type of entity (declared in MCLASS)
  public static final String ETYPE = "__ETYPE__";
  protected final PluginConfigurations config;
  protected final Writer writer;
  private final MClassRegistry registry;
  private final ValidationUtil validationUtil;

  public MetadataWriter(PluginConfigurations config,
                        Writer writer) {
    this.config = config;
    this.writer = writer;
    registry = new MClassRegistry();
    validationUtil = new ValidationUtil();
  }

  /**
   * Mark that writing metadata entities and relationships are about to start
   */
  public abstract void begin();

  /**
   * Mark the end of metadata writing
   */
  public abstract void end();

  /**
   * Write the given entity based on @MProperty annotations.
   * Also create and write relations based on @MRelation annotations.
   * @param entity
   */
  public void write(Entity entity) {
    write(Collections.singleton(entity));
  }

  public void write(Collection<Entity> entities) {
    Collection<Map<String, Object>> mObjects = Lists.newLinkedList();
    for (Entity entity : entities) {
      mObjects.addAll(getAllMClasses(entity));
    }
    persistMetadataValues(mObjects);
  }

  protected abstract void persistMetadataValues(
      Collection<Map<String, Object>> values);

  /**
   * Flush the data that has been written but still not yet persisted
   */
  public void flush() {
    try {
      writer.flush();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * Close this writer
   */
  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    registry.reset();
  }

  /**
   * Retrieve the MProperty attributes from the given entity and create
   * appropriate MProperty attribute maps from MRelations and referred
   * entities
   *
   * @param entity
   * @return collection of converted MClass object attributes
   */
  protected Collection<Map<String, Object>> getAllMClasses(Entity entity) {
    Preconditions.checkNotNull(entity);
    Map<String, Map<String, Object>> idToValues = Maps.newHashMap();
    getAllMClasses(entity, idToValues);
    return idToValues.values();
  }

  private void getAllMClasses(Entity entity,
                              Map<String, Map<String, Object>> idToValues) {
    if (StringUtils.isEmpty(entity.getSourceId()) &&
        (entity instanceof CustomEntity)) {
      entity.setSourceId(MD5IdGenerator.generateIdentity(
          config.getApplicationUrl()));
    }
    if (StringUtils.isEmpty(entity.getIdentity())) {
      entity.setIdentity(entity.generateId());
    }

    validationUtil.validateRequiredMProperties(entity);
    if (!idToValues.containsKey(entity.getIdentity())) {
      idToValues.put(entity.getIdentity(), getMClassAttributes(entity));
      getMRelations(entity, idToValues);
    }
  }

  private void getMRelations(Entity entity,
                             Map<String, Map<String, Object>> idToValues) {
    // get all MRelation entries
    Collection<MRelationEntry> relationAttrs =
        registry.getRelations(entity.getClass());
    Relation rel;
    Collection<? extends Entity> referred;
    for (MRelationEntry relEntry : relationAttrs) {
      // getAllMClasses if there's a referred entity instead of a String id
      if (relEntry.isConnectedToEntity()) {
        referred = relEntry.getConnectedEntities(entity);
        for(Entity other : referred) {
          getAllMClasses(other, idToValues);
        }
      }
      rel = relEntry.buildRelation(entity, config.getNamespace());
      // add Relation attributes
      idToValues.put(rel.getIdentity(), getMClassAttributes(rel));
    }
  }

  private Map<String, Object> getMClassAttributes(Entity entity) {
    Map<String, Object> values = getAttributes(entity);
    values.put(MTYPE, Entity.MTYPE);
    values.put(ETYPE, entity.getClass().getCanonicalName());
    return values;
  }

  private Map<String, Object> getMClassAttributes(Relation rel) {
    Map<String, Object> values = getAttributes(rel);
    values.put(MTYPE, Relation.MTYPE);
    return values;
  }

  /**
   * Return a Map of MProperty attributes to appropriate values from the given
   * MClass annotated object
   *
   * @param mClassObj
   * @return
   */
  private Map<String, Object> getAttributes(Object mClassObj) {
    Preconditions.checkArgument(mClassObj.getClass()
        .isAnnotationPresent(MClass.class));
    Collection<MPropertyEntry> properties =
        registry.getProperties(mClassObj.getClass());
    Map<String, Object> valueMap = Maps.newHashMap();
    for (MPropertyEntry prop : properties) {
      valueMap.put(prop.getAttribute(), prop.getValue(mClassObj));
    }
    return valueMap;
  }
}
