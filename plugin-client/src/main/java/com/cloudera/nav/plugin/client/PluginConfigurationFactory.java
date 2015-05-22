package com.cloudera.nav.plugin.client;

import com.google.common.base.Throwables;

import java.net.URI;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Create PluginConfiguration instance
 */
public class PluginConfigurationFactory {

  // expected property names
  public static final String APP_URL = "application_url";
  public static final String FILE_FORMAT = "file_format";
  public static final String METADATA_URI = "metadata_parent_uri";
  public static final String NAMESPACE = "namespace";
  public static final String NAV_URL = "navigator_url";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";

  /**
   * Create a PluginConfiguration from the properties contained in the
   * given filePath
   *
   * @param filePath
   * @return
   */
  public PluginConfigurations readConfigurations(String filePath) {
    try {
      PropertiesConfiguration props = new PropertiesConfiguration(filePath);
      PluginConfigurations config = new PluginConfigurations();
      config.setApplicationUrl(props.getString(APP_URL));
      config.setFileFormat(FileFormat.valueOf(
          props.getString(FILE_FORMAT, FileFormat.JSON.name())));
      config.setMetadataParentUri(URI.create(props.getString(METADATA_URI)));
      config.setNamespace(props.getString(NAMESPACE));
      config.setNavigatorUrl(props.getString(NAV_URL));
      config.setUsername(props.getString(USERNAME));
      config.setPassword(props.getString(PASSWORD));
      return config;
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
  }
}
