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
package com.cloudera.nav.plugin.model.entities;

import com.cloudera.nav.plugin.model.HdfsIdGenerator;
import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.annotations.MProperty;

/**
 * A concrete entity that represents HDFS directories or files. Note that the
 * source type and namespace should not be modified.
 */
@MClass(validTypes = {EntityType.DIRECTORY, EntityType.FILE})
public class HdfsEntity extends Entity {

  @MProperty
  private String fileSystemPath;

  public HdfsEntity() {
    setSourceType(SourceType.HDFS);
    setNamespace(NAVIGATOR);
  }

  public HdfsEntity(String sourceId, String fileSystemPath, EntityType type) {
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
    this.fileSystemPath = fileSystemPath;
  }
}
