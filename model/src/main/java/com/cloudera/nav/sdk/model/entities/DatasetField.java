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

import com.cloudera.nav.sdk.model.DatasetIdGenerator;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;

public abstract class DatasetField extends SchemaElement {

  @MProperty
  private Integer fieldIndex;
  @MProperty(attribute = "firstClassParentId")
  private String parentId;

  public DatasetField() {
    setEntityType(EntityType.FIELD);
  }

  @Override
  public String generateId() {
    return DatasetIdGenerator.fieldId(getParentId(), getName());
  }

  /**
   * @return if available, the position of the field within its dataset
   */
  public Integer getFieldIndex() {
    return fieldIndex;
  }

  /**
   * @return the id of the parent dataset that contains this field
   */
  public String getParentId() {
    return parentId;
  }

  public void setFieldIndex(Integer fieldIndex) {
    this.fieldIndex = fieldIndex;
  }

  /**
   * Change the id of the parent dataset
   * @param parent the parent dataset entity
   */
  public void setParentId(Entity parent) {
    setParentId(parent.getIdentity());
  }

  /**
   * *
   * @param parentId
   */
  public void setParentId(String parentId) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(parentId),
        "DatasetField parent must have valid identity");
    this.parentId = parentId;
  }
}
