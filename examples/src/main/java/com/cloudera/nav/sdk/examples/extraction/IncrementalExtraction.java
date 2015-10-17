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
import com.cloudera.nav.sdk.client.MetadataResultSet;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.google.common.base.Preconditions;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

/**
 * Sample program that runs incremental extraction. Includes
 * sample marker for string formatting, and option to read/write marker to a
 * file. Log output shows total number of entities and relations extracted, and
 * how many pages/batches of results were iterated through.
 *
 * Program arguments:
 * 1. path to config file: see examples/src/main/resources/sample.conf
 * 2. output file path: where to write the extracted marker for next run
 * 3. start marker (optional): path to saved marker
 * 4. end marker (optional): path to saved marker
 *
 * The marker is a string cursor return by the server when extraction occurs.
 * By giving a start/end marker, only metadata between the start/end markers
 * are retrieved
 */
public class IncrementalExtraction {

  public static void main(String[] args) throws IOException {
    // handle arguments
    Preconditions.checkArgument(args.length >= 2);
    String configFilePath = args[0];
    String markerWritePath = args[1];
    String startMarker = args.length > 2 ? readFileArg(args[2]) : null;
    String endMarker = args.length > 3 ? readFileArg(args[3]) : null;

    NavApiCient client = NavigatorPlugin.fromConfigFile(configFilePath)
        .getClient();

    // Extract metadata
    MetadataExtractor extractor = new MetadataExtractor(client, null);
    MetadataResultSet rs = extractor.extractMetadata(startMarker, endMarker);

    // Save the marker to be used next time
    String nextMarker = rs.getMarker();
    try (PrintWriter markerWriter = new PrintWriter(markerWritePath, "UTF-8")) {
      markerWriter.println(nextMarker);
    }

    // Iterate through entities and relations and process them

    Iterator<Map<String, Object>> entitiesIt = rs.getEntities().iterator();
    Integer totalEntities = 0;
    while(entitiesIt.hasNext()) {
      processNextResult(entitiesIt.next());
      totalEntities++;
    }

    Iterator<Map<String, Object>> relationsIt = rs.getRelations().iterator();
    Integer totalRelations = 0;
    while(relationsIt.hasNext()){
      processNextResult(relationsIt.next());
      totalRelations++;
    }

    System.out.println("Total number of entities: " + totalEntities);
    System.out.println("Total number of relations: " + totalRelations);
    System.out.println("Next marker: " + nextMarker);
  }

  private static void processNextResult(Map<String, Object> metadataObject) {
    // pass
  }

  static String readFileArg(String path) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
      return reader.readLine();
    }
  }
}
