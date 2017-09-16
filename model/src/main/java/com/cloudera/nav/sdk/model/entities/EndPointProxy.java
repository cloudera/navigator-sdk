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

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.google.common.base.Strings;

import java.util.Map;

/**
 * A proxy for an Entity to be used as a Relation end-point.
 * It either has an identity or the atrributes required to generate the
 * identity in addition to the source type and the entity type. For entities
 * types which require a source id, the source id needs to be explicitly set
 * in the EndPointProxy.
 * The remainder of the information either already is on the server or
 * will be populated by the server
 */
@MClass(model="proxy")
public class EndPointProxy extends Entity {

  private Map<String, String> endPointAttributes;
  public EndPointProxy(Map<String, String> endPointAttributes,
                       SourceType sourceType,
                       EntityType entityType) {
    this.endPointAttributes = endPointAttributes;
    setSourceType(sourceType);
    setEntityType(entityType);
  }

  public EndPointProxy(String id, SourceType sourceType,
                       EntityType entityType) {
    setIdentity(id);
    setSourceType(sourceType);
    setEntityType(entityType);
  }

  public Map<String, String> getIdAttrsMap() {
    return endPointAttributes;
  }

  @Override
  public void validateEntity() {
    if (Strings.isNullOrEmpty(this.getIdentity()) &&
        this.getIdAttrsMap().isEmpty()) {
      throw new IllegalArgumentException(
          "Either the Entity Id or the EndPointAttributes plan must be " +
              "provided");
    }
  }
}