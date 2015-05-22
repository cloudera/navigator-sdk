package com.cloudera.nav.plugin.client;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import com.cloudera.nav.plugin.client.writer.MetadataWriter;
import com.cloudera.nav.plugin.client.writer.MetadataWriterFactory;
import com.cloudera.nav.plugin.model.entities.Entity;
import com.cloudera.nav.plugin.model.entities.HdfsEntity;
import com.google.common.collect.Iterables;

import java.net.URL;
import java.util.Collection;

import org.junit.*;
import org.junit.runner.*;
import org.mockito.*;
import org.mockito.runners.*;

@RunWith(MockitoJUnitRunner.class)
public class NavigatorPluginTest {

  private MetadataWriter mockWriter;
  private MetadataWriterFactory mockFactory;
  @Captor
  private ArgumentCaptor<Collection<Entity>> captor;

  @Before
  public void setUp() {
    mockWriter = mock(MetadataWriter.class);
    mockFactory = mock(MetadataWriterFactory.class);
    doReturn(mockWriter).when(mockFactory).newWriter(
        any(PluginConfigurations.class));
  }

  @Test
  public void testWrite() {
    URL url = this.getClass().getClassLoader().getResource("nav_plugin.conf");
    PluginConfigurationFactory factory = new PluginConfigurationFactory();
    PluginConfigurations config = factory.readConfigurations(url.getPath());
    NavigatorPlugin plugin = new NavigatorPlugin(config, mockFactory);
    HdfsEntity entity = new HdfsEntity();
    entity.setIdentity("foo");
    plugin.write(entity);
    verify(mockWriter).write(captor.capture());
    assertEquals(Iterables.getOnlyElement(captor.getValue()), entity);
    verify(mockWriter).flush();
    verify(mockWriter).close();
  }
}
