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

public class HdfsEntityTest {

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidHdfsEntity() {
    HdfsEntity hdfsEntity = new HdfsEntity();
    hdfsEntity.validateEntity();
  }

  /**
   * Creating the HDFSEntity using the file system path
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidHdfsEntitySourceIdMissing() {
    HdfsEntity hdfsEntity = new HdfsEntity();
    hdfsEntity.setFileSystemPath("/user/hdfs");
    hdfsEntity.validateEntity();
  }

  /**
   * Creating the HDFSEntity using the identity
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidHdfsEntitySourceIdAbsent2() {
    HdfsEntity hdfsEntity2 = new HdfsEntity();
    hdfsEntity2.setIdentity("HDFS");
    hdfsEntity2.validateEntity();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidHdfsEntityFileSystemPathMissing() {
    HdfsEntity hdfsEntity = new HdfsEntity();
    hdfsEntity.setSourceId("1");
    hdfsEntity.validateEntity();
  }

  @Test
  public void testValidHdfsEntity() {
    HdfsEntity hdfsEntity = new HdfsEntity();
    hdfsEntity.setSourceId("1");
    hdfsEntity.setFileSystemPath("/user/hdfs");
    hdfsEntity.validateEntity();

    // Specifying the id instead of filesystem path
    HdfsEntity hdfsEntity2 = new HdfsEntity();
    hdfsEntity2.setSourceId("1");
    hdfsEntity2.setIdentity("HDFS");
    hdfsEntity2.validateEntity();
  }
}
