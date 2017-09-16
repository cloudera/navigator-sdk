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
package com.cloudera.nav.sdk.model.entities;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

@MClass(model="pig_operation", validTypes = {EntityType.OPERATION})
public class PigOperation extends Entity {
  private final String JOB_NAME = "jobName";
  private final String LOGICAL_PLAN_HASH = "logicalPlanHash";

  @MProperty
  private String jobName;

  @MProperty
  private String logicalPlanHash;

  public PigOperation() {
    setSourceType(SourceType.PIG);
    setEntityType(EntityType.OPERATION);
  }

  public PigOperation(String logicalPlanHash, String jobName) {
    this();
    setLogicalPlanHash(logicalPlanHash);
    setJobName(jobName);
  }

  public PigOperation(String id) {
    this();
    setIdentity(id);
  }

  @Override
  public void validateEntity() {
    if (Strings.isNullOrEmpty(this.getIdentity()) &&
        (Strings.isNullOrEmpty(this.getJobName()) ||
        Strings.isNullOrEmpty(this.getLogicalPlanHash())))
      throw new IllegalArgumentException(
          "Either the Entity Id or the jobname and the logical plan must be " +
              "provided");
  }

  public void setLogicalPlanHash(String logicalPlanHash) {
    this.logicalPlanHash = logicalPlanHash;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getJobName() {
    return this.jobName;
  }

  public String getLogicalPlanHash() {
    return this.logicalPlanHash;
  }

  public Map<String, String> getIdAttrsMap() {
    return ImmutableMap.of(
        JOB_NAME, this.getJobName(),
        LOGICAL_PLAN_HASH, this.getLogicalPlanHash());
  }
}