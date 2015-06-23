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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.httpclient.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Write metadata in JSON format over an HTTP connection
 */
public class HttpJsonMetadataWriter extends MetadataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(
      HttpJsonMetadataWriter.class);

  private final HttpURLConnection conn;
  private int count;

  public HttpJsonMetadataWriter(PluginConfigurations config, Writer writer,
                                HttpURLConnection conn) {
    super(config, writer);
    this.conn = conn;
    count = 0;
  }

  @Override
  public void begin() {
    try {
      writer.append("[");
      count = 0;
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public void end() {
    try {
      writer.append("]");
    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  @Override
  protected void persistMetadataValues(Collection<Map<String, Object>> values) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      for (Map<String, Object> json : values) {
        if (count > 0) {
          writer.append(",");
        }
        writer.append(mapper.writeValueAsString(json));
        count++;
      }
    } catch (IOException e) {
      Throwables.propagate(e);
    }
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
