// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.model.entities;

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.annotations.MProperty;

/**
 * A concrete entity that represents HDFS directories or files
 */
@MClass(validTypes = {EntityType.DIRECTORY, EntityType.FILE})
public class HdfsEntity extends Entity {

  private String fileSystemPath;
  private EntityType entityType;

  /**
   * An HDFS file/directory can be uniquely identified by the path and
   * the Source id
   * @return
   */
  @Override
  protected String[] getIdComponents() {
    return new String[] { getSourceId(), getFileSystemPath() };
  }

  /**
   * @return the full path for this file or directory
   */
  @MProperty(required=true)
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

  @Override
  @MProperty(required=true)
  public SourceType getSourceType() {
    return SourceType.HDFS;
  }

  @Override
  @MProperty
  public EntityType getType() {
    return entityType;
  }

  public void setType(EntityType entityType) {
    this.entityType = entityType;
  }
}
