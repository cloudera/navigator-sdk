package com.cloudera.nav.sdk.model.entities;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

@MClass(model="pig_operation", validTypes = {EntityType.OPERATION})
public class PigOperation extends Entity {
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
        "jobName", this.getJobName(),
        "logicalPlanHash", this.getLogicalPlanHash());
  }
}