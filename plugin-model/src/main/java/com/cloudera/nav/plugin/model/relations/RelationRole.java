// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.model.relations;

/**
 * Specifies the role that the related entity plays in a relationship.
 */
public enum RelationRole {

  /**
   * Source of the relationship in a directional relationship like DATA_FLOW.
   */
  SOURCE(EndPoint.ENDPOINT1),
  /**
   * Target of the relationship in a directional relationship like DATA_FLOW.
   */
  TARGET(EndPoint.ENDPOINT2),
  /**
   * Parent entity for a PARENT_CHILD relationship.
   */
  PARENT(EndPoint.ENDPOINT1),
  /**
   * Children entities for a PARENT_CHILD relationship.
   */
  CHILD(EndPoint.ENDPOINT2),
  /**
   * Logical entities for a LOGICAL_PHYSICAL relationship.
   */
  LOGICAL(EndPoint.ENDPOINT1),
  /**
   * Physical entity for a LOGICAL_PHYSICAL relationship.
   */
  PHYSICAL(EndPoint.ENDPOINT2),
  /**
   * Template entity for a INSTANCE_OF relationship.
   */
  TEMPLATE(EndPoint.ENDPOINT1),
  /**
   * Instance entity for a INSTANCE_OF relationship.
   */
  INSTANCE(EndPoint.ENDPOINT2),
  /**
   * One end of a conjoint relationship.
   */
  ENDPOINT1(EndPoint.ENDPOINT1),
  /**
   * Other end of a conjoint relationship.
   */
  ENDPOINT2(EndPoint.ENDPOINT2);

  private final EndPoint endPoint;

  RelationRole(EndPoint endPoint) {
    this.endPoint = endPoint;
  }

  public EndPoint getEndPoint() {
    return endPoint;
  }

  public RelationRole getInverseRole() {
    switch (this) {
      case SOURCE:
        return RelationRole.TARGET;
      case TARGET:
        return RelationRole.SOURCE;
      case PARENT:
        return RelationRole.CHILD;
      case CHILD:
        return RelationRole.PARENT;
      case PHYSICAL:
        return RelationRole.LOGICAL;
      case LOGICAL:
        return RelationRole.PHYSICAL;
      case TEMPLATE:
        return RelationRole.INSTANCE;
      case INSTANCE:
        return RelationRole.TEMPLATE;
      case ENDPOINT1:
        return RelationRole.ENDPOINT2;
      case ENDPOINT2:
        return RelationRole.ENDPOINT1;
      default:
        throw new IllegalArgumentException("Unknown role " +
            String.valueOf(this));
    }
  }

  public static enum EndPoint {
    ENDPOINT1,
    ENDPOINT2
  }
}