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

package com.cloudera.nav.plugin.examples.stetson;

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.cloudera.nav.plugin.model.annotations.MRelation;
import com.cloudera.nav.plugin.model.entities.CustomEntity;
import com.cloudera.nav.plugin.model.entities.EndPointProxy;
import com.cloudera.nav.plugin.model.entities.Entity;
import com.cloudera.nav.plugin.model.entities.EntityType;
import com.cloudera.nav.plugin.model.relations.RelationRole;
import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
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
  private Entity pigExecution; // MD5(pig.script.id) from the job conf
  private String link;

  public StetsonExecution(String namespace) {
    // Because the namespace is given to input/output we ensure it
    // exists when it is used by adding it as a c'tor parameter
    Preconditions.checkArgument(StringUtils.isNotEmpty(namespace));
    setNamespace(namespace);
  }

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
    return new String[] { getNamespace(),
        getTemplate().getIdentity(),
        getPigExecution().getIdentity() };
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
  @MRelation(role=RelationRole.PHYSICAL)
  public Entity getPigExecution() {
    return pigExecution;
  }

  public void setTemplate(StetsonScript template) {
    this.template = template;
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

  public void setPigExecution(String pigExecutionId) {
    this.pigExecution = new EndPointProxy(pigExecutionId, SourceType.PIG,
        EntityType.OPERATION_EXECUTION);
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
