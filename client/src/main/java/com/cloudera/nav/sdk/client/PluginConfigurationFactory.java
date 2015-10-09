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

import com.google.common.base.Throwables;

import java.net.URI;
import java.util.Map;

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
      config.setFormat(Format.valueOf(
          props.getString(FILE_FORMAT, Format.JSON.name())));
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

  public PluginConfigurations fromConfigMap(Map<String, Object> props) {
    PluginConfigurations config = new PluginConfigurations();
    config.setApplicationUrl(props.get(APP_URL).toString());
    Format format = props.containsKey(FILE_FORMAT) ?
        Format.valueOf(props.get(FILE_FORMAT).toString()) :
        Format.JSON;
    config.setFormat(format);
    config.setMetadataParentUri(URI.create(props.get(METADATA_URI).toString()));
    config.setNamespace(props.get(NAMESPACE).toString());
    config.setNavigatorUrl(props.get(NAV_URL).toString());
    config.setUsername(props.get(USERNAME).toString());
    config.setPassword(props.get(PASSWORD).toString());
    return config;
  }
}
