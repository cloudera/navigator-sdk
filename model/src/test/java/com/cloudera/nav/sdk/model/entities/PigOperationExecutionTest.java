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

import org.junit.Test;

public class PigOperationExecutionTest {
  @Test
  public void testValidPigOperationExecution() {
    PigOperationExecution pigOperationExecution = new PigOperationExecution();
    pigOperationExecution.setJobName("Job_Name");
    pigOperationExecution.setScriptId("Script_id");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPigOperationExecution() {
    PigOperationExecution pigOperationExecution = new PigOperationExecution();
    pigOperationExecution.validateEntity();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPigOperationExecutionJobNameMissing() {
    PigOperationExecution pigOperationExecution = new PigOperationExecution();
    pigOperationExecution.setScriptId("Script_id");
    pigOperationExecution.validateEntity();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPigOperationExecutionScriptIdMissing() {
    PigOperationExecution pigOperationExecution = new PigOperationExecution();
    pigOperationExecution.setJobName("Job_Name");
    pigOperationExecution.validateEntity();
  }
}
