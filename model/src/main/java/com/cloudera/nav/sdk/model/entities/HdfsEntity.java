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
package com.cloudera.nav.sdk.model.entities;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * A concrete entity that represents HDFS directories or files. Note that the
 * source type and namespace should not be modified.
 */
@MClass(model="fselement", validTypes = {EntityType.DIRECTORY, EntityType.FILE})
public class HdfsEntity extends Entity {
  @MProperty
  private String fileSystemPath;

  public HdfsEntity() {
    setSourceType(SourceType.HDFS);
  }

  public HdfsEntity(String fileSystemPath, EntityType type, String sourceId) {
    this();
    setSourceId(sourceId);
    setFileSystemPath(fileSystemPath);
    setEntityType(type);
  }

  public HdfsEntity(String id) {
    this();
    setIdentity(id);
  }

  @Override
  public void validateEntity() {
    if (Strings.isNullOrEmpty(this.getIdentity()) &&
        Strings.isNullOrEmpty(this.getFileSystemPath())) {
      throw new IllegalArgumentException(
          "Either the Entity Id or file system path used" +
              " to generate the id must be present");
    }
  }

  public void setFileSystemPath(String fileSystemPath) {
    this.fileSystemPath = fileSystemPath;
  }

  public String getFileSystemPath() {
    return this.fileSystemPath;
  }

  @Override
  public Map<String, String> getIdAttrsMap() {
    return ImmutableMap.of("fileSystemPath", this.getFileSystemPath());
  }
}
