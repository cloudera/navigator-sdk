// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
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