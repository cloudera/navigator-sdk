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

import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.cloudera.nav.plugin.model.annotations.MRelation;
import com.cloudera.nav.plugin.model.relations.RelationRole;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * Defines a logical dataset that encapsulates the schema
 */
public abstract class Dataset extends SchemaElement {

  @MRelation(role= RelationRole.CHILD)
  private Collection<DatasetField> fields;

  @MProperty
  private String datasetType;
  @MProperty
  private List<String> partitionColNames;
  @MProperty
  private List<String> partitionTypes;
  @MProperty
  private CompressionType compressionType;
  @MProperty
  private FileFormat fileFormat;

  public Dataset() {
    setEntityType(EntityType.DATASET);
  }

  /**
   * @return a application specific dataset type designator
   */
  public String getDatasetType() {
    return datasetType;
  }

  public void setDatasetType(String datasetType) {
    this.datasetType = datasetType;
  }

  /**
   * @return schema fields belonging to this dataset
   */
  public Collection<? extends Entity> getFields() {
    return fields;
  }

  /**
   * Set the schema fields belonging to this dataset
   * @param fields
   */
  public void setFields(Collection<? extends DatasetField> fields) {
    if (getIdentity() == null) {
      setIdentity(generateId());
    }
    for (DatasetField field : fields) {
      field.setParentId(this);
    }
    this.fields = Lists.newLinkedList(fields);
  }

  /**
   * @return field names used in partitions
   */
  public List<String> getPartitionColNames() {
    return partitionColNames;
  }

  public void setPartitionColNames(List<String> partitionColNames) {
    this.partitionColNames = partitionColNames;
  }

  /**
   * @return the partition type
   */
  public List<String> getPartitionTypes() {
    return partitionTypes;
  }

  public void setPartitionTypes(List<String> partitionTypes) {
    this.partitionTypes = partitionTypes;
  }

  /**
   * @return data compression type, if any
   */
  public CompressionType getCompressionType() {
    return compressionType;
  }

  public void setCompressionType(CompressionType compressionType) {
    this.compressionType = compressionType;
  }

  /**
   * @return file format information, if applicable
   */
  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat(FileFormat fileFormat) {
    this.fileFormat = fileFormat;
  }
}
