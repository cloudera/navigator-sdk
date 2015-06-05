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

package com.cloudera.nav.plugin.client.examples.stetson2;

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.cloudera.nav.plugin.model.annotations.MRelation;
import com.cloudera.nav.plugin.model.entities.CustomEntity;
import com.cloudera.nav.plugin.model.entities.EndPointProxy;
import com.cloudera.nav.plugin.model.entities.Entity;
import com.cloudera.nav.plugin.model.entities.EntityType;
import com.cloudera.nav.plugin.model.relations.RelationRole;

/**
 * This is a custom logical dataset that is physically backed by an
 * HDFS directory
 */
@MClass
public class StetsonDataset extends CustomEntity {

  private Entity hdfsEntity;

  /**
   * @param name Stetson name for the dataset
   * @param namespace
   * @param hdfsEntityId identity of the underlying HDFS directory
   */
  public StetsonDataset(String name, String namespace, String hdfsEntityId) {
    setName(name);
    setNamespace(namespace);
    setHdfsEntity(hdfsEntityId);
  }

  @Override
  @MProperty
  public EntityType getType() {
    return EntityType.DATASET;
  }

  @MRelation(role= RelationRole.PHYSICAL)
  public Entity getHdfsEntity() {
    return hdfsEntity;
  }

  @Override
  protected String[] getIdComponents() {
    return new String[] { getName(), getNamespace(),
        getHdfsEntity().getIdentity() };
  }

  public void setHdfsEntity(String hdfsEntityId) {
    hdfsEntity = new EndPointProxy(hdfsEntityId, SourceType.HDFS,
        EntityType.DIRECTORY);
  }
}
