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
package com.cloudera.nav.plugin.client;

import com.cloudera.nav.plugin.client.writer.MetadataWriter;
import com.cloudera.nav.plugin.client.writer.MetadataWriterFactory;
import com.cloudera.nav.plugin.model.entities.Entity;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Collection;

import org.apache.commons.lang.StringUtils;

/**
 * Communicates with Navigator to register custom entity models and
 * write enriched data to be combined with Navigator's lineage and metadata
 * information
 */
public class NavigatorPlugin {

  private final PluginConfigurations config;
  private final MetadataWriterFactory factory;

  /**
   * The plugin must be configured with the URL for the Navigator API and the
   * location for saving new metadata information
   *
   * @param config
   * @param factory
   */
  public NavigatorPlugin(PluginConfigurations config,
                         MetadataWriterFactory factory) {
    Preconditions.checkArgument(!StringUtils.isEmpty(config.getNavigatorUrl()));
    Preconditions.checkArgument(config.getMetadataParentUri() != null);
    Preconditions.checkNotNull(factory);
    this.config = config;
    this.factory = factory;
  }

  /**
   * The plugin must be configured with the URL for the Navigator API and the
   * location for saving new metadata information
   *
   * @param config
   */
  public NavigatorPlugin(PluginConfigurations config) {
    this(config, new MetadataWriterFactory());
  }

  /**
   * Currently this is unsupported. Instead, a generic custom entity is
   * being used by Navigator.
   *
   * Search for classes defined using the @MClass annotation
   * in the given package. Registers all found classes with Navigator
   * @param packageName
   */
  public void registerModels(String packageName) {
    throw new UnsupportedOperationException();
  }

  /**
   * Currently this is unsupported. Instead, a generic custom entity is
   * being used by Navigator.
   *
   * Register a single class with Navigator. It must have the @MClass
   * annotation
   * @param entityClass
   */
  public void registerModel(Class<? extends Entity> entityClass) {
    throw new UnsupportedOperationException();
  }

  /**
   * Write the custom entity
   * @param entity
   */
  public void write(Entity entity) {
    write(ImmutableList.of(entity));
  }

  /**
   * Write a collection of custom entities
   * @param entities
   */
  public void write(Collection<Entity> entities) {
    MetadataWriter writer = factory.newWriter(config);
    try {
      writer.begin();
      writer.write(entities);
      writer.end();
      writer.flush();
    } finally {
      writer.close();
    }
  }
}
