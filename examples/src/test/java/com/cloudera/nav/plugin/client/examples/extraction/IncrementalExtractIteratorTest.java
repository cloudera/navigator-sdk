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

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.cloudera.nav.plugin.client.EntityResultsBatch;
import com.cloudera.nav.plugin.client.NavApiCient;
import com.cloudera.nav.plugin.client.QueryCriteria;
import com.cloudera.nav.plugin.client.RelationResultsBatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Unit tests for IncrementalExtractIterator
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class IncrementalExtractIteratorTest {
  private NavApiCient client;
  private EntityResultsBatch entityBatch;
  private RelationResultsBatch relationBatch;

  @Before
  public void setUp(){
    client = mock(NavApiCient.class);
    List<Map<String, Object>> result = Lists.newArrayList();
    entityBatch = new EntityResultsBatch();
    entityBatch.setCursorMark("nextCursor");
    entityBatch.setResults(result);
    relationBatch = new RelationResultsBatch();
    relationBatch.setCursorMark("nextCursor");
    relationBatch.setResults(result);
    when(client.getEntityBatch((QueryCriteria) argThat(new QueryArgumentsMatcher())))
        .thenReturn(entityBatch);
    when(client.getRelationBatch((QueryCriteria) argThat(new QueryArgumentsMatcher())))
        .thenReturn(relationBatch);
  }

  protected static class QueryArgumentsMatcher extends ArgumentMatcher{
    public boolean matches(Object o){
      return (o instanceof QueryCriteria);
    }
  }

  @Test(expected = NoSuchElementException.class)
  public void testNextNone(){
    List<String> extractorRunIds = Lists.newArrayList("x##0", "x##1", "x##2");
    IncrementalExtractIterator incrementalExtractIterator = new IncrementalExtractIterator(client,
        MetadataType.ENTITIES,"identity:*", 100, extractorRunIds);
    Map<String, Object> next = incrementalExtractIterator.next();
  }

  @Test
  public void testNextEntities(){
    List<String> extractorRunIds = Lists.newArrayList("x##0", "x##1", "x##2");
    Map<String, Object> singleResult = Maps.newHashMap();
    singleResult.put("field", "value");
    entityBatch.setResults(Lists.newArrayList(singleResult));
    IncrementalExtractIterator incrementalExtractIterator = new IncrementalExtractIterator(client,
        MetadataType.ENTITIES,"identity:*", 100, extractorRunIds);
    assertEquals(entityBatch.getResults().get(0), incrementalExtractIterator.next());
  }

  @Test
  public void testNextRelations(){
    List<String> extractorRunIds = Lists.newArrayList("x##0", "x##1", "x##2");
    Map<String, Object> singleResult = Maps.newHashMap();
    singleResult.put("field", "value");
    relationBatch.setResults(Lists.newArrayList(singleResult));
    IncrementalExtractIterator incrementalExtractIterator = new IncrementalExtractIterator(client,
        MetadataType.RELATIONS, "identity:*", 100, extractorRunIds);
    assertEquals(relationBatch.getResults().get(0), incrementalExtractIterator.next());
  }

  @Test
  public void testGetNextDocs(){
    List<String> extractorRunIds = Lists.newArrayList("x##0", "x##1", "x##2");
    Map<String, Object> singleResult = Maps.newHashMap();
    singleResult.put("field", "value");
    entityBatch.setResults(Lists.newArrayList(singleResult));
    IncrementalExtractIterator incrementalExtractIterator = new IncrementalExtractIterator(client,
        MetadataType.ENTITIES, "identity:*", 100, extractorRunIds);
    incrementalExtractIterator.getNextBatch();
    assertTrue(incrementalExtractIterator.hasNext());
  }
}

