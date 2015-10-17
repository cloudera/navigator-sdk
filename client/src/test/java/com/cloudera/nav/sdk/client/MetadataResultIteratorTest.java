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
package com.cloudera.nav.sdk.client;

import static org.junit.Assert.*;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

import com.cloudera.nav.sdk.model.MetadataType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

/**
 * Unit tests for IncrementalExtractIterator
 */
@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("unchecked")
public class MetadataResultIteratorTest {
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
    when(client.getEntityBatch((MetadataQuery) argThat(new QueryArgumentsMatcher())))
        .thenReturn(entityBatch);
    when(client.getRelationBatch((MetadataQuery) argThat(new QueryArgumentsMatcher())))
        .thenReturn(relationBatch);
  }

  protected static class QueryArgumentsMatcher extends ArgumentMatcher{
    public boolean matches(Object o){
      return (o instanceof MetadataQuery);
    }
  }

  @Test(expected = NoSuchElementException.class)
  public void testNextNone(){
    List<String> extractorRunIds = Lists.newArrayList("x##0", "x##1", "x##2");
    MetadataResultIterator metadataResultIterator = new MetadataResultIterator(client,
        MetadataType.ENTITIES,"identity:*", 100, extractorRunIds);
    Map<String, Object> next = metadataResultIterator.next();
  }

  @Test
  public void testNextEntities(){
    List<String> extractorRunIds = Lists.newArrayList("x##0", "x##1", "x##2");
    Map<String, Object> singleResult = Maps.newHashMap();
    singleResult.put("field", "value");
    entityBatch.setResults(Lists.newArrayList(singleResult));
    MetadataResultIterator metadataResultIterator = new MetadataResultIterator(client,
        MetadataType.ENTITIES,"identity:*", 100, extractorRunIds);
    assertEquals(entityBatch.getResults().get(0), metadataResultIterator.next());
  }

  @Test
  public void testNextRelations(){
    List<String> extractorRunIds = Lists.newArrayList("x##0", "x##1", "x##2");
    Map<String, Object> singleResult = Maps.newHashMap();
    singleResult.put("field", "value");
    relationBatch.setResults(Lists.newArrayList(singleResult));
    MetadataResultIterator metadataResultIterator = new MetadataResultIterator(client,
        MetadataType.RELATIONS, "identity:*", 100, extractorRunIds);
    assertEquals(relationBatch.getResults().get(0), metadataResultIterator.next());
  }

  @Test
  public void testGetNextDocs(){
    List<String> extractorRunIds = Lists.newArrayList("x##0", "x##1", "x##2");
    Map<String, Object> singleResult = Maps.newHashMap();
    singleResult.put("field", "value");
    entityBatch.setResults(Lists.newArrayList(singleResult));
    MetadataResultIterator metadataResultIterator = new MetadataResultIterator(client,
        MetadataType.ENTITIES, "identity:*", 100, extractorRunIds);
    metadataResultIterator.getNextBatch();
    assertTrue(metadataResultIterator.hasNext());
  }
}

