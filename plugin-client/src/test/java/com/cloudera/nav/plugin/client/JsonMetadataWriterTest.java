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

package com.cloudera.nav.plugin.client;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.cloudera.nav.plugin.client.writer.JsonMetadataWriter;
import com.cloudera.nav.plugin.client.writer.MetadataWriter;
import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.entities.Entity;
import com.cloudera.nav.plugin.model.entities.EntityType;
import com.cloudera.nav.plugin.model.entities.HdfsEntity;
import com.cloudera.nav.plugin.model.relations.DataFlowRelation;
import com.cloudera.nav.plugin.model.relations.Relation;
import com.cloudera.nav.plugin.model.relations.RelationIdGenerator;
import com.cloudera.nav.plugin.model.relations.RelationType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.junit.*;

public class JsonMetadataWriterTest {

  private ByteArrayOutputStream stream;
  private PluginConfigurations config;
  private HttpURLConnection mockConn;

  @Before
  public void setUp() throws IOException {
    stream = new ByteArrayOutputStream();
    mockConn = mock(HttpURLConnection.class);
    doReturn(200).when(mockConn).getResponseCode();
    config = mock(PluginConfigurations.class);
    doReturn("test").when(config).getNamespace();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWriteEntity() throws IOException {
    Source source = new Source("HDFS-1", SourceType.HDFS, "Cluster",
        "http://ns1");
    HdfsEntity entity = new HdfsEntity();
    entity.setSourceId(source.getIdentity());
    entity.setFileSystemPath("/user/test");
    entity.setEntityType(EntityType.DIRECTORY);
    entity.setTags(ImmutableList.of("foo", "bar"));

    JsonMetadataWriter mWriter = new JsonMetadataWriter(config, stream,
        mockConn);
    mWriter.write(entity);

    String value = new String(stream.toByteArray());
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> values = (Map<String, Object>)mapper.readValue(value,
        Map[].class)[0];
    assertEquals(values.get("identity"), entity.getIdentity());
    assertEquals(values.get("fileSystemPath"), entity.getFileSystemPath());
    assertEquals(values.get("sourceId"), source.getIdentity());
    assertEquals(values.get("sourceType"), SourceType.HDFS.name());
    assertEquals(values.get("type"), EntityType.DIRECTORY.name());
    assertEquals(values.get("deleted"), false);
    assertTrue(CollectionUtils.isEqualCollection(
        (Collection<String>) values.get("tags"),
        ImmutableList.of("foo", "bar")));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWriteRelation() throws IOException {
    Source source = new Source("HDFS-1", SourceType.HDFS, "Cluster",
        "http://ns1");
    HdfsEntity inputData = new HdfsEntity(source.getIdentity(),
        "/user/test/input", EntityType.DIRECTORY);
    inputData.setTags(ImmutableList.of("foo", "bar"));

    HdfsEntity outputData = new HdfsEntity(source.getIdentity(),
        "/user/test/output", EntityType.DIRECTORY);
    outputData.setTags(ImmutableList.of("foo", "bar"));

    Relation rel = DataFlowRelation.builder()
        .idGenerator(new RelationIdGenerator())
        .source(inputData)
        .target(outputData)
        .namespace("test")
        .build();

    JsonMetadataWriter mWriter = new JsonMetadataWriter(config, stream,
        mockConn);
    mWriter.writeRelation(rel);

    String value = new String(stream.toByteArray());
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> values = (Map<String, Object>)mapper.readValue(value,
        Map[].class)[0];
    assertEquals(values.get("identity"), rel.getIdentity());
    assertEquals(values.get("type"), RelationType.DATA_FLOW.name());
    assertEquals(values.get("ep1SourceId"), source.getIdentity());
    assertEquals(values.get("ep2SourceId"), source.getIdentity());
    assertEquals(values.get("ep1SourceType"), SourceType.HDFS.name());
    assertEquals(values.get("ep2SourceType"), SourceType.HDFS.name());
    assertEquals(values.get("ep1Type"), EntityType.DIRECTORY.name());
    assertEquals(values.get("ep2Type"), EntityType.DIRECTORY.name());
    assertEquals(Iterables.getOnlyElement(
            (Collection<String>) values.get("ep1Ids")),
        inputData.getIdentity());
    assertEquals(Iterables.getOnlyElement(
            (Collection<String>) values.get("ep2Ids")),
        outputData.getIdentity());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWriteComposite() throws IOException {
    Source source = new Source("ExternalApp", SourceType.PLUGIN, "ExternalApp",
        "http://appHost:port");
    CustomOperationExecution exec = prepExec(source);
    JsonMetadataWriter mWriter = new JsonMetadataWriter(config, stream,
        mockConn);
    mWriter.write(exec);

    ObjectMapper mapper = new ObjectMapper();
    String value = new String(stream.toByteArray());
    Map<String, Object>[] values = mapper.readValue(value, Map[].class);

    int relationCount = 0;
    int entityCount = 0;
    String mtype;
    for (Map<String, Object> v : values) {
      mtype = v.get(MetadataWriter.MTYPE).toString();
      if (mtype.equals(Entity.MTYPE)) {
        entityCount++;
      } else if (mtype.equals(Relation.MTYPE)) {
        relationCount++;
      } else {
        throw new AssertionError("Unrecognized metadata type " +
            String.valueOf(mtype));
      }
    }
    // custom op and exec
    assertEquals(entityCount, 2);
    // custom op -> pig op, custom exec -> pig exec, custom op -> exec
    assertEquals(relationCount, 3);
  }

  private CustomOperationExecution prepExec(Source source) {
    CustomOperation op = new CustomOperation();
    op.setName("JobName");
    op.setPigOperationId("pigOperationId");
    op.setScript("LOAD data; DoStuff(data)");
    op.setNamespace("test");
    op.setSourceId(source.getIdentity());
    op.setOwner("owner");
    op.setIdentity(op.generateId());
    CustomOperationExecution exec = new CustomOperationExecution();
    exec.setPigExecutionId("pigExecId");
    exec.setTemplate(op);
    exec.setNamespace(config.getNamespace());
    exec.setSourceId(source.getIdentity());
    return exec;
  }

}
