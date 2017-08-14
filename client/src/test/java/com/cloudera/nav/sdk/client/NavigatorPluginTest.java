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
import static org.mockito.Mockito.*;

import com.cloudera.nav.sdk.client.writer.MetadataWriter;
import com.cloudera.nav.sdk.client.writer.MetadataWriterFactory;
import com.cloudera.nav.sdk.model.MetadataModel;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.HdfsEntity;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.net.URL;
import java.util.Collection;
import java.util.Map;

import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

@RunWith(MockitoJUnitRunner.class)
public class NavigatorPluginTest {

  private MetadataWriter mockWriter;
  private MetadataWriterFactory mockFactory;
  @Captor
  private ArgumentCaptor<Collection<Entity>> captor;
  private ClientConfig config;
  private NavigatorPlugin plugin;

  @Before
  public void setUp() {
    mockWriter = mock(MetadataWriter.class);
    mockFactory = mock(MetadataWriterFactory.class);
    doReturn(mockWriter).when(mockFactory).newWriter();
    URL url = this.getClass().getClassLoader().getResource("nav_plugin.conf");
    ClientConfigFactory factory = new ClientConfigFactory();
    config = factory.readConfigurations(url.getPath());
    plugin = spy(new NavigatorPlugin(config, mockFactory));
  }

  @Test
  public void testWrite() {
    HdfsEntity entity = new HdfsEntity();
    entity.setIdentity("foo");
    plugin.write(entity);
    verify(mockWriter).write(captor.capture());
    assertEquals(Iterables.getOnlyElement(captor.getValue()), entity);
    verify(mockWriter).flush();
    verify(mockWriter).close();
  }

  @Test
  public void testRegisterModels() {
    assertModelRegistration();
  }

  private void assertModelRegistration() {
    RestTemplate mockTemplate = mock(RestTemplate.class);
    NavApiCient client = spy(plugin.getClient());
    when(plugin.getClient()).thenReturn(client);
    when(client.newRestTemplate()).thenReturn(mockTemplate);
    MetadataModel mockResponse = new MetadataModel();
    Map<String, Collection<String>> mockErrs = Maps.newHashMap();
    mockErrs.put("model1", Sets.newHashSet("msg1", "msg2"));
    when(mockTemplate.exchange(eq(plugin.getClient().getApiUrl() + "/models"),
        eq(HttpMethod.POST), any(HttpEntity.class), eq(MetadataModel.class)))
        .thenReturn(new ResponseEntity<>(mockResponse, HttpStatus.OK));
    MetadataModel response = plugin.registerModel(TestMClass.class);
    assertEquals(mockResponse, response);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRegisterModelsError() {
    // v7 doesn't support model registration
    config.setApiVersion(7);
    plugin = spy(new NavigatorPlugin(config, mockFactory));
    assertModelRegistration();
  }

  @MClass(model="test")
  private static class TestMClass extends Entity {
    @Override
    public String generateId() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void validateEntity() {};
  }
}
