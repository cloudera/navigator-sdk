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

package com.cloudera.nav.sdk.model.relations;

import com.cloudera.nav.sdk.model.MClassUtil;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

public class RelationValidator {

  Map<Field, Method> mclassInfo;

  public RelationValidator() {
    mclassInfo = MClassUtil.getAnnotatedProperties(Relation.class,
        MProperty.class);
  }

  public void validateRequiredMProperties(Relation relation) {
    Object value;
    for (Map.Entry<Field, Method> entry : mclassInfo.entrySet()) {
      if (entry.getKey().getAnnotation(MProperty.class).required()) {
        try {
          value = entry.getValue().invoke(relation);
          Preconditions.checkArgument(value != null,
              String.format("Required property %s was null",
                  entry.getKey().getName()));
          if (value instanceof Collection) {
            Preconditions.checkArgument(CollectionUtils.isNotEmpty(
                    (Collection)value),
                String.format("Required property %s was null",
                    entry.getKey().getName()));
          }
        } catch (IllegalAccessException e) {
          Throwables.propagate(e);
        } catch (InvocationTargetException e) {
          Throwables.propagate(e);
        }
      }
    }
  }

  public void validatateRelation(Relation relation) {
    Preconditions.checkArgument(
        CollectionUtils.isNotEmpty(relation.getEp1Ids()) ||
        CollectionUtils.isNotEmpty(relation.getEp1Attributes()),
        "Either ep1Ids or ep1Attributes must be present" );
    Preconditions.checkArgument(
        CollectionUtils.isNotEmpty(relation.getEp2Ids()) ||
            CollectionUtils.isNotEmpty(relation.getEp2Attributes()),
        "Either ep2Ids or ep2Attributes must be present" );
  }
}
