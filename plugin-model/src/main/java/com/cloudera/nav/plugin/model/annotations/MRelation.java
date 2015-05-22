// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.model.annotations;

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.relations.RelationRole;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Identifies a getter method as a Relation
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface MRelation {
  RelationRole role();
  SourceType sourceType() default SourceType.NONE;
  boolean required() default false;
}
