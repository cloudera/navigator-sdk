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

import static org.junit.Assert.*;

import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.entities.Dataset;
import com.cloudera.nav.sdk.model.entities.TagChangeSet;
import com.cloudera.nav.sdk.model.entities.UDPChangeSet;
import com.google.common.collect.Iterables;

import java.lang.reflect.Field;
import java.util.Collections;

import org.junit.*;

/**
 * Test MetadataModel and factory class
 */
public class MetadataModelTest {

  private MetadataModelFactory factory;

  @Before
  public void setUp() {
    factory = new MetadataModelFactory();
  }

  @Test
  public void testFactory() {
    MetadataModel model = factory.newModel(Collections.singleton(
        TestDataset.class), "test");
    assertEquals("test", Iterables.getOnlyElement(model.getNamespaces())
        .getName());
    assertEquals("test", Iterables.getOnlyElement(model.getPackages())
        .getName());
    assertEquals("dataset", Iterables.getOnlyElement(model.getClasses())
        .getName());
    int fieldCount = countMPropertyFields(TestDataset.class);
    assertEquals(fieldCount, model.getProperties().size());
  }

  private int countMPropertyFields(Class<?> aClass) {
    int fieldCount = 0;
    if (aClass == null) {
      return 0;
    }
    for (Field field : aClass.getDeclaredFields()) {
      MProperty ann = field.getAnnotation(MProperty.class);
      if (ann != null && ann.register() &&
          field.getType() != UDPChangeSet.class &&
          field.getType() != TagChangeSet.class) {
        fieldCount++;
      }
    }
    return fieldCount + countMPropertyFields(aClass.getSuperclass());
  }

  @MClass(model="dataset")
  private static class TestDataset extends Dataset {
    @Override
    public void validateEntity() {
      throw new UnsupportedOperationException();
    }

  }
}
