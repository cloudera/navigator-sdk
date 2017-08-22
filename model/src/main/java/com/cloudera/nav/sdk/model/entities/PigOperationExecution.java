package com.cloudera.nav.sdk.model.entities;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

@MClass(model="pig_op_exec", validTypes = {EntityType.OPERATION_EXECUTION})
public class PigOperationExecution extends Entity {

  @MProperty
  private String jobName;

  @MProperty
  private String scriptId;

  public PigOperationExecution() {
    setSourceType(SourceType.PIG);
    setEntityType(EntityType.OPERATION_EXECUTION);
  }

  public PigOperationExecution(String id) {
    this();
    setIdentity(id);
  }

  public PigOperationExecution(String scriptId, String jobName) {
    this();
    setScriptId(scriptId);
    setJobName(jobName);
  }

  @Override
  public void validateEntity() {
    if (Strings.isNullOrEmpty(this.getIdentity()) &&
        (Strings.isNullOrEmpty(this.getJobName()) ||
            Strings.isNullOrEmpty(this.getScriptId())))
      throw new IllegalArgumentException(
          "Either the Entity Id or the jobname and the script id must be " +
              "provided");
  }

  public void setScriptId(String scriptId) {
    this.scriptId = scriptId;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getJobName() {
    return this.jobName;
  }

  public String getScriptId() {
    return this.scriptId;
  }

  @Override
  public Map<String, String> getIdAttrsMap() {
    return ImmutableMap.of(
        "jobName", this.getJobName(),
        "scriptId", this.getScriptId());
  }
}
