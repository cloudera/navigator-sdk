package com.cloudera.nav.sdk.model.entities;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.google.common.base.Strings;

@MClass(model="pig_operation", validTypes = {EntityType.OPERATION})
public class PigOperation extends Entity {

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
}