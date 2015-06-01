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

package com.cloudera.nav.plugin.client.examples.stetson;

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.cloudera.nav.plugin.model.annotations.MRelation;
import com.cloudera.nav.plugin.model.entities.CustomEntity;
import com.cloudera.nav.plugin.model.entities.EntityType;
import com.cloudera.nav.plugin.model.relations.RelationRole;

import org.joda.time.Instant;

/**
 * Represents a specific execution of a hypothetical custom application
 * represented by a StetsonScript
 */
@MClass
public class StetsonExecution extends CustomEntity {

  private StetsonScript template;
  private Instant started;
  private Instant ended;
  // MD5(pig.script.id) from the job conf
  private String pigExecutionId;
  private String stetsonInstId;
  private String link;

  /**
   * Stetson executions are defined to be operation execution entities
   */
  @Override
  @MProperty
  public EntityType getType() {
    return EntityType.OPERATION_EXECUTION;
  }

  /**
   * The execution is uniquely identified by the the template's id and
   * the external application's identifier
   */
  @Override
  protected String[] getIdComponents() {
    return new String[] { getTemplate().getIdentity(),
        getStetsonInstId() };
  }

  @MProperty
  public String getLink() {
    return link;
  }

  /**
   * The custom DSL template
   */
  @MRelation(role = RelationRole.TEMPLATE)
  public StetsonScript getTemplate() {
    return template;
  }

  /**
   * The Pig execution id
   */
  @MRelation(role = RelationRole.PHYSICAL, sourceType = SourceType.PIG)
  public String getPigExecutionId() {
    return pigExecutionId;
  }

  public void setTemplate(StetsonScript template) {
    this.template = template;
  }

  /**
   * The external application identifier for this execution
   */
  @MProperty
  public String getStetsonInstId() {
    return stetsonInstId;
  }

  /**
   * Start time of execution in milliseconds since epoch
   */
  @MProperty
  public Instant getStarted() {
    return started;
  }

  /**
   * End time of execution in milliseconds since epoch
   */
  @MProperty
  public Instant getEnded() {
    return ended;
  }

  public void setPigExecutionId(String pigExecutionId) {
    this.pigExecutionId = pigExecutionId;
  }

  public void setStetsonInstId(String stetsonInstId) {
    this.stetsonInstId = stetsonInstId;
  }

  public void setStarted(Instant started) {
    this.started = started;
  }

  public void setEnded(Instant ended) {
    this.ended = ended;
  }

  public void setLink(String link) {
    this.link = link;
  }
}
