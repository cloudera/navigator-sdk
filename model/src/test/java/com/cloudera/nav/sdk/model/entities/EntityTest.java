package com.cloudera.nav.sdk.model.entities;

import org.junit.Test;

public class EntityTest {

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidHdfsEntity() throws  Exception {
    HdfsEntity hdfsEntity = new HdfsEntity();
    hdfsEntity.validateEntity();
  }

  @Test
  public void testValidHdfsEntity() {
    HdfsEntity hdfsEntity = new HdfsEntity();
    hdfsEntity.setFileSystemPath("/user/hdfs");
    hdfsEntity.validateEntity();

    HdfsEntity hdfsEntity2 = new HdfsEntity();
    hdfsEntity2.setIdentity("Hdfs_Identity");
    hdfsEntity2.validateEntity();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPigOperation() {
    PigOperation pigOperation = new PigOperation();
    pigOperation.validateEntity();
  }

  @Test
  public void testValidPigOperation() {
    PigOperation pigOperation = new PigOperation();
    pigOperation.setJobName("Job_Name");
    pigOperation.setLogicalPlanHash("Logical_Plan_Hash");
    pigOperation.validateEntity();

    PigOperation pigOperation2 = new PigOperation();
    pigOperation2.setIdentity("pig_id");
    pigOperation2.validateEntity();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPigOperationExecution() throws Exception {
    PigOperationExecution pigOperationExecution = new PigOperationExecution();
    pigOperationExecution.validateEntity();
  }

  @Test
  public void testValidPigOperationExecution() throws Exception {
    PigOperationExecution pigOperationExecution = new PigOperationExecution();
    pigOperationExecution.setJobName("Job_Name");
    pigOperationExecution.setScriptId("Script_id");
  }
}
