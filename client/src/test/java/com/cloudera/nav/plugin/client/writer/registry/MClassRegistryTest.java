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

import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.entities.EntityType;
import com.google.common.collect.ImmutableList;

import org.junit.*;

/**
 * Test MClassRegistry
 */
public class MClassRegistryTest {

  private MetadataClass mclassObj;
  private MClassRegistry registry;

  @Before
  public void setUp() {
    registry = new MClassRegistry();
    mclassObj = new MetadataClass();
    mclassObj.setIdentity("id");
    mclassObj.setSourceType(SourceType.HDFS);
    mclassObj.setEntityType(EntityType.DIRECTORY);
    mclassObj.setNamespace("namespace");
    mclassObj.setColl(ImmutableList.of("foo", "bar"));
    mclassObj.setIntField(5);
    mclassObj.setLongField(10L);
    mclassObj.setStrField("foo");
  }

  @Test
  public void testBasic() {
    registry.validateRequiredMProperties(mclassObj);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadColl() {
    mclassObj.setColl(null);
    registry.validateRequiredMProperties(mclassObj);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadStr() {
    mclassObj.setStrField(null);
    registry.validateRequiredMProperties(mclassObj);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadObj() {
    mclassObj.setLongField(null);
    registry.validateRequiredMProperties(mclassObj);
  }

}
