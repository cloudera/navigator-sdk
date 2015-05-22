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
  public void close() {
    super.close();
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
