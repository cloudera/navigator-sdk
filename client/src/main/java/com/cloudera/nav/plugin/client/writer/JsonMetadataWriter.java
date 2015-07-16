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

package com.cloudera.nav.plugin.client.writer;

import com.cloudera.nav.plugin.client.PluginConfigurations;
import com.cloudera.nav.plugin.client.writer.serde.EntitySerializer;
import com.cloudera.nav.plugin.client.writer.serde.RelationSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;

import org.apache.commons.httpclient.HttpStatus;

/**
 * Write metadata in JSON format
 */
public class JsonMetadataWriter extends MetadataWriter {

  private final HttpURLConnection conn;

  public JsonMetadataWriter(PluginConfigurations config,
                            OutputStream stream,
                            HttpURLConnection conn) {
    super(config, stream);
    this.conn = conn;
  }

  @Override
  protected void persistMetadataValues(MClassWrapper mclassWrapper) {
    try {
      ObjectMapper mapper = newMapper();
      mapper.writeValue(stream, mclassWrapper);
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  private ObjectMapper newMapper() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("MetadataSerializer");
    module.addSerializer(new EntitySerializer(registry));
    module.addSerializer(new RelationSerializer(registry));
    mapper.registerModule(module);
    return mapper;
  }

  @Override
  public void flush() {
    super.flush();
    try {
      // request is not sent until response code is requested
      if (conn.getResponseCode() >= HttpStatus.SC_BAD_REQUEST) {
        throw new RuntimeException(String.format(
            "Error writing metadata (code %s): %s", conn.getResponseCode(),
            conn.getResponseMessage()));
      }
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }
}
