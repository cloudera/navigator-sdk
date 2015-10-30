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
package com.cloudera.nav.sdk.client;

import com.cloudera.nav.sdk.client.writer.MetadataWriter;
import com.cloudera.nav.sdk.client.writer.MetadataWriterFactory;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.MetadataModel;
import com.cloudera.nav.sdk.model.MetadataModelFactory;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Communicates with Navigator to register custom entity models and
 * write enriched data to be combined with Navigator's lineage and metadata
 * information
 */
public class NavigatorPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(NavigatorPlugin
      .class);

  /**
   * Use the information contained in the given configuration file to
   * create a new NavigatorPlugin
   *
   * @param configFilePath local file system path of plugin configurations
   * @return a new NavigatorPlugin instance
   */
  public static NavigatorPlugin fromConfigFile(String configFilePath) {
    ClientConfig config = (new ClientConfigFactory())
        .readConfigurations(configFilePath);
    return new NavigatorPlugin(config);
  }

  public static NavigatorPlugin fromConfigMap(Map<String, Object> configMap) {
    ClientConfig config = (new ClientConfigFactory())
        .fromConfigMap(configMap);
    return new NavigatorPlugin(config);
  }

  private final ClientConfig config;
  private final MetadataWriterFactory factory;
  private final NavApiCient client;

  /**
   * The plugin must be configured with the URL for the Navigator API and the
   * location for saving new metadata information
   *
   * @param config
   * @param factory
   */
  public NavigatorPlugin(ClientConfig config,
                         MetadataWriterFactory factory) {
    Preconditions.checkArgument(!StringUtils.isEmpty(config.getNavigatorUrl()),
        "No Navigator URL configured");
    Preconditions.checkArgument(config.getApiVersion() >= 7,
        "Minimum API version supported is v7 for writing to Navigator");
    Preconditions.checkNotNull(factory);
    this.config = config;
    this.factory = factory;
    this.client = new NavApiCient(config);
  }

  /**
   * The plugin must be configured with the URL for the Navigator API and the
   * location for saving new metadata information
   *
   * @param config
   */
  public NavigatorPlugin(ClientConfig config) {
    this(config, new MetadataWriterFactory(config));
  }

  /**
   * Currently this is unsupported. Instead, a generic custom entity is
   * being used by Navigator.
   *
   * Search for classes defined using the @MClass annotation
   * in the given package. Registers all found classes with Navigator
   * @param packageName
   */
  @SuppressWarnings("unchecked")
  public MetadataModel registerModels(String packageName) {
    Reflections ref = new Reflections(packageName + ".");
    Set<Class<?>> types = ref.getTypesAnnotatedWith(MClass.class);
    Collection<Class<Entity>> modelClasses = Lists.newArrayListWithExpectedSize(
        types.size() + 1);
    for (Class<?> aClass : types) {
      Preconditions.checkArgument(Entity.class.isAssignableFrom(aClass));
      modelClasses.add((Class<Entity>)aClass);
    }
    if (modelClasses.size() > 0) {
      LOG.info("Registering models: {}", modelClasses);
      return registerModels(modelClasses);
    } else {
      LOG.info("No models to be registered in package {}", packageName);
      return null;
    }
  }

  /**
   * Currently this is unsupported. Instead, a generic custom entity is
   * being used by Navigator.
   *
   * Register a single class with Navigator. It must have the @MClass
   * annotation
   * @param entityClass
   */
  public MetadataModel registerModel(Class<? extends Entity> entityClass) {
    return registerModels(Collections.singleton(entityClass));
  }

  public MetadataModel registerModels(
      Collection<? extends Class<? extends Entity>> classes) {
    Preconditions.checkArgument(config.getApiVersion() >= 9,
        "Model registration not supported by API earlier than v9");
    MetadataModelFactory factory = new MetadataModelFactory();
    MetadataModel model = factory.newModel(classes, getConfig().getNamespace());
    return getClient().registerModels(model);
  }

  /**
   * Write the custom entity
   * @param entity
   */
  public ResultSet write(Entity entity) {
    return write(ImmutableList.of(entity));
  }

  /**
   * Write a collection of custom entities
   * @param entities
   */
  public ResultSet write(Collection<Entity> entities) {
    MetadataWriter writer = factory.newWriter();
    try {
      writer.write(entities);
      writer.flush();
    } finally {
      writer.close();
    }
    return writer.getLastResultSet();
  }

  /**
   * @return a client to communicate with the Navigator REST API
   */
  public NavApiCient getClient() {
    return client;
  }

  public ClientConfig getConfig() {
    return config;
  }

  public String getNamespace() {
    return config.getNamespace();
  }
}