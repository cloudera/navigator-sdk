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
package com.cloudera.nav.plugin.client.examples.extraction;

import com.cloudera.nav.plugin.client.NavApiCient;
import com.cloudera.nav.plugin.client.PluginConfigurationFactory;
import com.cloudera.nav.plugin.client.PluginConfigurations;
import com.google.common.base.Throwables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Example script for testing incremental extraction from a marker. Includes
 * sample marker for string formatting, and option to read/write marker to a
 * file. Log output shows total number of entities and relations extracted, and
 * how many pages/batches of results were iterated through.
 */
public class MetadataExtraction {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataExtraction.class);

  private static String readFileArg(String arg){
    String marker;
    try {
      File startMarkerFile = new File(arg);
      FileReader fr = new FileReader(startMarkerFile);
      BufferedReader markerReader = new BufferedReader(fr);
      marker = markerReader.readLine();
      markerReader.close();
      fr.close();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return marker;
  }
  public static void main(String[] args) {
    // setup the plugin and api client
    String configFilePath = args[0];
    PluginConfigurations config = (new PluginConfigurationFactory())
        .readConfigurations(configFilePath);
    //NavigatorPlugin plugin = new NavigatorPlugin(config);
    NavApiCient client = new NavApiCient(config);
    String startMarker;
    try {
      startMarker = readFileArg(args[1]);
    } catch (ArrayIndexOutOfBoundsException e){
      startMarker = null;
    }
    String endMarker;
    try {
      endMarker = readFileArg(args[2]);
    } catch (ArrayIndexOutOfBoundsException e){
      endMarker = null;
    }
    MetadataExtractor extractor = new MetadataExtractor(client, null);
    MetadataResultSet rs = extractor.extractMetadata(startMarker, endMarker);
    String nextMarker = rs.getMarker();
    String markerWritePath;
    try {
      markerWritePath = args[3];
      PrintWriter markerWriter = new PrintWriter(markerWritePath, "UTF-8");
      markerWriter.println(nextMarker);
      markerWriter.close();
    } catch(IOException e) {
      throw Throwables.propagate(e);
    } catch (ArrayIndexOutOfBoundsException e){
      LOG.error("please specify a file to write next marker to");
    }

    Iterable<Map<String, Object>> en = rs.getEntities();
    Iterator<Map<String, Object>> entitiesIterator = en.iterator();
    Integer totalEntities = 0;
    while(entitiesIterator.hasNext()){
      Map<String,Object> nextResult = entitiesIterator.next();
      //Data processing with nextResult
      totalEntities++;
    }
    Iterable<Map<String, Object>> rel = rs.getRelations();
    Iterator<Map<String, Object>> relationsIterator = rel.iterator();
    Integer totalRelations = 0;
    while(relationsIterator.hasNext()){
      Map<String,Object> nextResult = relationsIterator.next();
      //Data processing with nextResult
      totalRelations++;
    }

    LOG.info("Total number of entities: " + totalEntities);
    System.out.println("Total number of entities: " + totalEntities);
    LOG.info("Total number of relations: " + totalRelations);
    System.out.println("Total number of relations: " + totalRelations);
    LOG.info("Next Marker: " + nextMarker);
    System.out.println("Next marker: " + nextMarker);
  }
}
