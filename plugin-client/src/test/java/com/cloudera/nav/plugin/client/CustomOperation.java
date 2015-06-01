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

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.cloudera.nav.plugin.model.annotations.MRelation;
import com.cloudera.nav.plugin.model.entities.CustomEntity;
import com.cloudera.nav.plugin.model.entities.EntityType;
import com.cloudera.nav.plugin.model.relations.RelationRole;

/**
 * Represents a template defined by a script in a custom DSL
 */
@MClass
public class CustomOperation extends CustomEntity {

  private String pigOperationId;
  private String script;

  /**
   * Extend to include all fields that uniquely determine a custom entity
   */
  @Override
  protected String[] getIdComponents() {
    return new String[] { getName(), getOwner() };
  }

  @MRelation(role= RelationRole.PHYSICAL, sourceType= SourceType.PIG)
  public String getPigOperationId() {
    return pigOperationId;
  }

  @Override
  @MProperty
  public EntityType getType() {
    return EntityType.OPERATION;
  }

  public void setPigOperationId(String pigOperationId) {
    this.pigOperationId = pigOperationId;
  }

  @MProperty
  public String getScript() {
    return script;
  }

  public void setScript(String script) {
    this.script = script;
  }
}
