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

package com.cloudera.nav.plugin.model;

import static org.junit.Assert.*;

import com.cloudera.nav.plugin.model.annotations.MClass;
import com.cloudera.nav.plugin.model.entities.EndPointProxy;
import com.cloudera.nav.plugin.model.entities.Entity;
import com.cloudera.nav.plugin.model.entities.HdfsEntity;
import com.cloudera.nav.plugin.model.entities.HiveColumn;
import com.cloudera.nav.plugin.model.entities.HiveTable;
import com.google.common.collect.ImmutableList;

import java.util.Collection;

import org.junit.*;

public class MClassTest {

  @Test
  public void testDefaultMClassEntities() {
    Collection<Class<? extends Entity>> classes = ImmutableList.of(
        EndPointProxy.class, HdfsEntity.class, HiveColumn.class,
        HiveTable.class);
    for (Class<? extends Entity> aClass : classes) {
      assertTrue(aClass.isAnnotationPresent(MClass.class));
    }
  }
}
