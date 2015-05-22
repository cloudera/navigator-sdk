// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.model.annotations;

import com.cloudera.nav.plugin.model.entities.EntityType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Identifies a class or interface as a custom entity.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface MClass {

  /** Valid EntityTypes that can be represented by this MClass. **/
  EntityType[] validTypes() default {};
}
