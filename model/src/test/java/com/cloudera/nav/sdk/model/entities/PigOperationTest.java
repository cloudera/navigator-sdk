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

public class PigOperationTest {

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
  public void testInvalidPigOperationJobNameMissing() {
    PigOperation pigOperation = new PigOperation();
    pigOperation.setLogicalPlanHash("Logical_Plan_Hash");
    pigOperation.validateEntity();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidPigOperationLogicalPlanHashMissing() {
    PigOperation pigOperation = new PigOperation();
    pigOperation.setJobName("Job_Name");
    pigOperation.validateEntity();
  }
}
