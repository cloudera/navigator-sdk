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
package com.cloudera.nav.plugin.model.relations;

/**
 * Specify the type of relationship
 */
public enum RelationType {

  /**
   * Describes data flow relationship. e.g. relation from file to mapreduce
   * job.
   */
  DATA_FLOW(RelationRole.SOURCE, RelationRole.TARGET),

  /**
   * Describes parent child relationship. e.g. relation between directory and
   * file.
   */
  PARENT_CHILD(RelationRole.PARENT, RelationRole.CHILD),

  /**
   * Describes the relation between a logical entity and its physical entity.
   * e.g. relation between Hive Query and MR Job.
   */
  LOGICAL_PHYSICAL(RelationRole.LOGICAL, RelationRole.PHYSICAL),

  /**
   * Describes instance of relationship between a template and its instance.
   * e.g. Operation execution is instance of Operation.
   */
  INSTANCE_OF(RelationRole.TEMPLATE, RelationRole.INSTANCE);

  private final RelationRole endpoint1Role;
  private final RelationRole endpoint2Role;

  public RelationRole getEndpoint1Role() {
    return endpoint1Role;
  }

  public RelationRole getEndpoint2Role() {
    return endpoint2Role;
  }

  RelationType(RelationRole endpoint1Role,
               RelationRole endpoint2Role) {
    this.endpoint1Role = endpoint1Role;
    this.endpoint2Role = endpoint2Role;
  }
}