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

package com.cloudera.nav.sdk.model;

import com.cloudera.nav.sdk.model.custom.CustomProperty;
import com.cloudera.nav.sdk.model.custom.MetaClass;
import com.cloudera.nav.sdk.model.custom.MetaClassPackage;
import com.cloudera.nav.sdk.model.custom.Namespace;

import java.util.Map;
import java.util.Set;

/**
 * Encapsulates relevant model information about a class annotated with @MClass
 * so the model can be registered with the Navigator server
 */
public class MetadataModel {

  private Set<MetaClassPackage> packages;
  private Set<MetaClass> classes;
  private Set<Namespace> namespaces;
  private Set<CustomProperty> properties;
  private Map<String, Set<String>> mappings;

  public Set<MetaClassPackage> getPackages() {
    return packages;
  }

  public void setPackages(Set<MetaClassPackage> packages) {
    this.packages = packages;
  }

  public Set<MetaClass> getClasses() {
    return classes;
  }

  public void setClasses(Set<MetaClass> classes) {
    this.classes = classes;
  }

  public Set<Namespace> getNamespaces() {
    return namespaces;
  }

  public void setNamespaces(Set<Namespace> namespaces) {
    this.namespaces = namespaces;
  }

  public Set<CustomProperty> getProperties() {
    return properties;
  }

  public void setProperties(Set<CustomProperty> properties) {
    this.properties = properties;
  }

  /**
   * Map<package.class, Set<namespace.property>>
   */
  public Map<String, Set<String>> getMappings() {
    return mappings;
  }

  public void setMappings(Map<String, Set<String>> mappings) {
    this.mappings = mappings;
  }
}
