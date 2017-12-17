/*
 * Copyright (c) 2017 Cloudera, Inc.
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

package com.cloudera.nav.sdk.examples.lineage2;

import com.cloudera.nav.sdk.model.CustomIdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MRelation;
import com.cloudera.nav.sdk.model.entities.EndPointProxy;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.entities.HdfsEntity;
import com.cloudera.nav.sdk.model.relations.RelationRole;

import java.util.Collections;

/**
 * This is a custom logical dataset that is physically backed by an
 * HDFS directory
 */
@MClass(model = "stetson_dataset")
public class StetsonDataset extends Entity {

  @MRelation(role= RelationRole.PHYSICAL)
  private Entity hdfsEntity;

  /**
   * @param name Stetson name for the dataset
   * @param namespace
   * @param hdfsEntity the hdfsEntity object
   */
  public StetsonDataset(String name, String namespace, HdfsEntity hdfsEntity) {
    setName(name);
    setNamespace(namespace);
    setHdfsEntity(hdfsEntity);
  }

  @Override
  public SourceType getSourceType() {
    return SourceType.SDK;
  }

  @Override
  public EntityType getEntityType() {
    return EntityType.DATASET;
  }

  public Entity getHdfsEntity() {
    return hdfsEntity;
  }

  /**
   * Each dataset is uniquely identified by the namespace and the name of the
   * dataset
   */
  @Override
  public String generateId() {
    return CustomIdGenerator.generateIdentity(getName(), getNamespace(),
        hdfsEntity.getIdAttrsMap().toString());
  }

  public void setHdfsEntity(HdfsEntity hdfsEntity) {
    this.hdfsEntity = new EndPointProxy(hdfsEntity.getIdAttrsMap(), hdfsEntity
        .getSourceType(), hdfsEntity.getEntityType());

    // HDFS EndPoint requires a source id to be present
    this.hdfsEntity.setSourceId(hdfsEntity.getSourceId());
  }
}