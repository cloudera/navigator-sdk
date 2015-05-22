package com.cloudera.nav.plugin.client;

import static org.junit.Assert.assertEquals;

import java.net.URL;

import org.junit.*;

public class PluginConfigurationFactoryTest {

  @Test
  public void testCreateFromFile() {
    URL url = this.getClass().getClassLoader().getResource("nav_plugin.conf");
    PluginConfigurationFactory factory = new PluginConfigurationFactory();
    PluginConfigurations config = factory.readConfigurations(url.getPath());

    assertEquals(config.getApplicationUrl(), "http://external-app.com");
    assertEquals(config.getFileFormat(), FileFormat.JSON);
    assertEquals(config.getMetadataParentUri().toString(),
        "http://nav.cloudera.com:7187/api/v7/plugin");
    assertEquals(config.getNamespace(), "tf");
    assertEquals(config.getNavigatorUrl(),
        "http://nav.cloudera.com:7187/api/v7/");
    assertEquals(config.getUsername(), "username");
    assertEquals(config.getPassword(), "password");
  }
}
