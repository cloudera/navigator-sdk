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
package com.cloudera.nav.sdk.client.writer;

import com.cloudera.nav.sdk.client.ClientConfig;
import com.cloudera.nav.sdk.client.SSLUtils;
import com.google.common.base.Throwables;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import org.apache.commons.net.util.Base64;

/**
 * A factory that returns the appropriate MetadataWriter given a set of
 * configurations
 */
public class MetadataWriterFactory {

  private final ClientConfig config;
  private final boolean isSSL;
  private final SSLContext sslContext;
  private final HostnameVerifier hostnameVerifier;

  public MetadataWriterFactory(ClientConfig config) {
    this.config = config;
    this.isSSL = SSLUtils.isSSL(config.getNavigatorUrl());
    this.sslContext = isSSL ? SSLUtils.getSSLContext(config) : null;
    this.hostnameVerifier = isSSL ? SSLUtils.getHostnameVerifier(config) : null;
  }

  /**
   * Create a new metadata writer
   */
  public MetadataWriter newWriter() {
    try {
      HttpURLConnection conn = createHttpStream();
      OutputStream stream = new BufferedOutputStream(conn.getOutputStream());
      return new JsonMetadataWriter(config, stream, conn);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private HttpURLConnection createHttpStream()
      throws IOException {
    String apiUrl = joinUrlPath(
        joinUrlPath(config.getNavigatorUrl(),
            "api/v" + String.valueOf(config.getApiVersion())),
            "metadata/plugin");
    HttpURLConnection conn = openConnection(new URL(apiUrl));
    conn.setRequestMethod("POST");
    String userpass = config.getUsername() + ":" + config.getPassword();
    String basicAuth = "Basic " + new String(Base64.encodeBase64(
        userpass.getBytes()));
    conn.addRequestProperty("Authorization", basicAuth);
    conn.addRequestProperty("Content-Type", "application/json");
    conn.setDoOutput(true);
    return conn;
  }

  private HttpURLConnection openConnection(URL url)
      throws IOException {
    if (isSSL) {
      HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
      conn.setHostnameVerifier(hostnameVerifier);
      conn.setSSLSocketFactory(sslContext.getSocketFactory());
      return conn;
    } else {
      return (HttpURLConnection) url.openConnection();
    }
  }

  private static String joinUrlPath(String base, String component) {
    return base + (base.endsWith("/") ? "" : "/") + component;
  }
}