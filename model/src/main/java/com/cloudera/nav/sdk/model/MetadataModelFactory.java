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

import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.custom.CustomProperty;
import com.cloudera.nav.sdk.model.custom.MetaClass;
import com.cloudera.nav.sdk.model.custom.MetaClassPackage;
import com.cloudera.nav.sdk.model.custom.Namespace;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.TagChangeSet;
import com.cloudera.nav.sdk.model.entities.UDPChangeSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * Factory for generating MetadataModel instances from Entity sub-types
 * annotated with @MClass
 */
public class MetadataModelFactory {

  /**
   * Create a MetadataModel from entity subclasses and a given namespace.
   * For now we're assuming the given namespace is used for both the
   * package of the meta-classes and the custom properties
   *
   * @param classes
   * @param namespace
   */
  public MetadataModel newModel(
      Collection<? extends Class<? extends Entity>> classes,
      String namespace) {
    MetadataModel model = new MetadataModel();
    MetaClassPackage pkg = MetaClassPackage.newPackage(namespace);
    model.setPackages(Sets.newHashSet(pkg));
    Namespace ns = Namespace.newNamespace(namespace);
    model.setNamespaces(Sets.newHashSet(ns));

    Set<MetaClass> metaClasses = Sets.newHashSet();
    Map<String, CustomProperty> mPropertyMap = Maps.newHashMap();
    Map<String, Set<String>> mappings = Maps.newHashMap();
    MetaClass mClass;
    for (Class<? extends Entity> aClass : classes) {
      MClass ann = aClass.getAnnotation(MClass.class);
      mClass = MetaClass.newClass(pkg.getName(), ann.model());
      metaClasses.add(mClass);

      // get all @MProperty's (including inherited ones)
      Map<Field, Method> properties = MClassUtil.getAnnotatedProperties(
          aClass, MProperty.class);

      Class<?> valueType;
      Set<String> classMappings = Sets.newHashSet();
      for (Map.Entry<Field, Method> entry : properties.entrySet()) {
        valueType = entry.getKey().getType();
        MProperty pAnn = entry.getKey().getAnnotation(MProperty.class);
        if (valueType != UDPChangeSet.class && valueType != TagChangeSet.class
            && pAnn.register()) {
          String pName = StringUtils.isEmpty(pAnn.attribute()) ?
              entry.getKey().getName() : pAnn.attribute();
          boolean multiValued = Collection.class.isAssignableFrom(valueType);
          if (checkExitingProperties(pName, pAnn, multiValued, mPropertyMap)) {
            mPropertyMap.put(pName, CustomProperty.newProperty(ns.getName(),
                pName, pAnn.fieldType(), multiValued, pAnn.values()));
            classMappings.add(ns.getName() + "." + pName);
          }
        }
      }
      mappings.put(pkg.getName() + "." + mClass.getName(), classMappings);
    }
    model.setClasses(metaClasses);
    model.setProperties(Sets.newHashSet(mPropertyMap.values()));
    model.setMappings(mappings);
    return model;
  }

  private boolean checkExitingProperties(String pName, MProperty ann,
                                         boolean multiValued,
                                         Map<String, CustomProperty> mProps) {
    CustomProperty existing = mProps.get(pName);
    if (existing != null) {
      Preconditions.checkArgument(ann.register());
      Preconditions.checkArgument(ann.fieldType() ==
          existing.getPropertyType(),
          String.format("Expecting %s to be of type %s, got %s instead",
              pName, existing.getPropertyType(), ann.fieldType()));
      // TODO validate type constraints
      Preconditions.checkArgument(multiValued == existing.isMultiValued(),
          String.format("Expecting %s to%sbe multi-valued", pName,
              multiValued ? " " : " not "));
    }
    return existing == null;
  }
}
