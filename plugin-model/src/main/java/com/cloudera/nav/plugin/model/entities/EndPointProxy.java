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

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.annotations.MClass;

/**
 * A proxy for an Entity to be used as a Relation end-point.
 * It only has an identity, source type, and entity type.
 * The remainder of the information either already is on the server or
 * will be populated by the server
 */
@MClass(model="proxy")
public class EndPointProxy extends Entity {

  public EndPointProxy(String id, SourceType sourceType, EntityType type) {
    setIdentity(id);
    setSourceType(sourceType);
    setEntityType(type);
  }

  /**
   * Throws UnsupportedOperationException.
   * The entity id for a proxy must be set explicitly
   */
  @Override
  public String generateId() {
    throw new UnsupportedOperationException();
  }
}
