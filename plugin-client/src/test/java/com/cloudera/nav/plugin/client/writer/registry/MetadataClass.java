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

package com.cloudera.nav.plugin.client.writer.registry;

import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.cloudera.nav.plugin.model.entities.Entity;

import java.util.Collection;

@MClass
public class MetadataClass extends Entity {

  @MProperty(required = true)
  private Collection<String> coll;
  @MProperty(required = true)
  private int intField;
  @MProperty(required = true)
  private String strField;
  @MProperty(required = true)
  private Long longField;
  @MProperty
  private String optional;

  public Collection<String> getColl() {
    return coll;
  }

  public void setColl(Collection<String> coll) {
    this.coll = coll;
  }

  public int getIntField() {
    return intField;
  }

  public void setIntField(int intField) {
    this.intField = intField;
  }

  public String getStrField() {
    return strField;
  }

  public void setStrField(String strField) {
    this.strField = strField;
  }

  public Long getLongField() {
    return longField;
  }

  public void setLongField(Long longField) {
    this.longField = longField;
  }

  public String getOptional() {
    return optional;
  }

  public void setOptional(String optional) {
    this.optional = optional;
  }

  @Override
  public String generateId() {
    return null;
  }
}
