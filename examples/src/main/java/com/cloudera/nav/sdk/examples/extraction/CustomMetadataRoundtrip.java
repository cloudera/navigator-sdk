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

package com.cloudera.nav.sdk.examples.extraction;

import com.cloudera.nav.sdk.client.MetadataExtractor;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.HiveTable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Extract metadata entities and modify custom metadata
 */
public class CustomMetadataRoundtrip {

  public static void main(String[] args) throws IOException {
    Preconditions.checkArgument(args.length == 1);
    String configFilePath = args[0];
    NavigatorPlugin sdk = NavigatorPlugin.fromConfigFile(configFilePath);

    Collection<Entity> toWrite = Lists.newLinkedList();
    HiveTable table;
    for (Map<String, Object> extractedEntity : getHiveTables("sample*", sdk)) {
      // Identify table
      table = new HiveTable();
      table.setIdentity(extractedEntity.get("identity").toString());

      // Set custom metadata
      table.setDescription("a different description");
      toWrite.add(table);
    }
    sdk.write(toWrite);
  }

  private static Iterable<Map<String, Object>> getHiveTables(
      String tableExpr, NavigatorPlugin sdk) {
    String query = "sourceType:HIVE AND type:TABLE and originalName:" +
        tableExpr;
    MetadataExtractor extractor = new MetadataExtractor(sdk.getClient(), null);
    return extractor.extractMetadata(null, null, query, null).getEntities();
  }
}
