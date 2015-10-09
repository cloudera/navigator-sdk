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

import com.cloudera.nav.sdk.model.HdfsIdGenerator;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;

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
    setIdentity(generateId());
  }

  /**
   * An HDFS file/directory can be uniquely identified by the path and
   * the Source id
   *
   * @return
   */
  @Override
  public String generateId() {
    return HdfsIdGenerator.generateHdfsEntityId(getSourceId(),
        getFileSystemPath());
  }

  /**
   * @return the full path for this file or directory
   */
  public String getFileSystemPath() {
    return fileSystemPath;
  }

  /**
   * Set the full path of this file or directory
   * @param fileSystemPath
   */
  public void setFileSystemPath(String fileSystemPath) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(fileSystemPath));
    if (fileSystemPath.endsWith("/")) {
      fileSystemPath = fileSystemPath.substring(0, fileSystemPath.length() - 1);
    }
    this.fileSystemPath = fileSystemPath;
  }
}
