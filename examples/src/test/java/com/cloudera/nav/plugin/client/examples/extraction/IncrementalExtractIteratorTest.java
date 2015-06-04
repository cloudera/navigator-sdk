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

package com.cloudera.nav.plugin.client.examples.extraction;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.cloudera.nav.plugin.client.EntityResultsBatch;
import com.cloudera.nav.plugin.client.MetadataType;
import com.cloudera.nav.plugin.client.NavApiCient;
import com.cloudera.nav.plugin.client.PluginConfigurations;
import com.cloudera.nav.plugin.client.ResultsBatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Unit tests for IncrementalExtractIterator
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class IncrementalExtractIteratorTest {
  private NavApiCient client;
  private ResultsBatch resultsBatch;
  private IncrementalExtractIterator incrementalExtractIterator;

  @Before
  public void setUp(){
    URL url = this.getClass().getClassLoader().getResource("nav_plugin.conf");
    PluginConfigurations config =new PluginConfigurations();
    config.setUsername("admin");
    config.setPassword("admin");
    config.setNavigatorUrl("navigator/url");
    client = mock(NavApiCient.class);
    resultsBatch = new EntityResultsBatch();
    List<Map<String, Object>> result = Lists.newArrayList();
    resultsBatch.setCursorMark("nextCursor");
    resultsBatch.setResults(result);
    when(client.getResultsBatch(eq(MetadataType.ENTITIES), anyString(), anyString(), anyInt()))
        .thenReturn(resultsBatch);
  }

  @Test(expected = NoSuchElementException.class)
  public void testNextNone(){
    List<String> extractorRunIds = Lists.newArrayList("x##0", "x##1", "x##2");
    incrementalExtractIterator = new IncrementalExtractIterator(client, MetadataType.ENTITIES,
        "identity:*", 100, extractorRunIds);
    Map<String, Object> next = incrementalExtractIterator.next();
  }

  @Test
  public void testNextWithResult(){
    List<String> extractorRunIds = Lists.newArrayList("x##0", "x##1", "x##2");
    Map<String, Object> singleResult = Maps.newHashMap();
    singleResult.put("field", "value");
    resultsBatch.setResults(Lists.newArrayList(singleResult));
    incrementalExtractIterator = new IncrementalExtractIterator(client, MetadataType.ENTITIES,
        "identity:*", 100, extractorRunIds);
    assertEquals(resultsBatch.getResults().get(0), incrementalExtractIterator.next());
  }

  @Test
  public void testGetNextDocs(){
    List<String> extractorRunIds = Lists.newArrayList("x##0", "x##1", "x##2");
    Map<String, Object> singleResult = Maps.newHashMap();
    singleResult.put("field", "value");
    resultsBatch.setResults(Lists.newArrayList(singleResult));
    incrementalExtractIterator = new IncrementalExtractIterator(client, MetadataType.ENTITIES,
        "identity:*", 100, extractorRunIds);
    incrementalExtractIterator.getNextBatch();
    assertTrue(incrementalExtractIterator.hasNext());
  }
}

