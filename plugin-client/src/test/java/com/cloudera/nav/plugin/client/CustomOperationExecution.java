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

import com.cloudera.nav.plugin.model.MD5IdGenerator;
import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.cloudera.nav.plugin.model.annotations.MRelation;
import com.cloudera.nav.plugin.model.entities.CustomEntity;
import com.cloudera.nav.plugin.model.entities.EntityType;
import com.cloudera.nav.plugin.model.relations.RelationRole;

/**
 * Represents a specific execution of a StetsonScript
 */
@MClass
public class CustomOperationExecution extends CustomEntity {

  private CustomOperation template;
  private Long startTime;
  // MD5(pig.script.id) from the job conf
  private String pigExecutionId;
  private String customOperationInstanceId;

  @Override
  @MProperty
  public EntityType getType() {
    return EntityType.OPERATION_EXECUTION;
  }

  @Override
  public String generateId() {
    return MD5IdGenerator.generateIdentity(getTemplate().getIdentity(),
        getCustomOperationInstanceId());
  }

  @MRelation(role = RelationRole.INSTANCE)
  public CustomOperation getTemplate() {
    return template;
  }

  public void setTemplate(CustomOperation template) {
    this.template = template;
  }

  @MRelation(role = RelationRole.PHYSICAL, sourceType = SourceType.PIG)
  public String getPigExecutionId() {
    return pigExecutionId;
  }

  public void setPigExecutionId(String pigExecutionId) {
    this.pigExecutionId = pigExecutionId;
  }

  @MProperty
  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  @MProperty
  public String getCustomOperationInstanceId() {
    return customOperationInstanceId;
  }

  public void setCustomOperationId(String customOperationInstanceId) {
    this.customOperationInstanceId = customOperationInstanceId;
  }
}
