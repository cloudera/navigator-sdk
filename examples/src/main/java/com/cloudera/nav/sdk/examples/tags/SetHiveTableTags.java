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

package com.cloudera.nav.sdk.examples.tags;

import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.HiveTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.Collection;

import org.apache.commons.collections.CollectionUtils;

/**
 * Tagging Hive Tables
 *
 * Tags is an important part of business metadata. This example uses the
 * Navigator plugin to tag a Hive column as sensitive.
 * Users and applications can then the tags to trigger actions such as
 * encryption, masking, and/or restrictions to permissions.
 */
public class SetHiveTableTags {

  public static void main(String[] args) {

    // setup the plugin and api client
    NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile(args[0]);

    // send tags for multiple entities to Navigator
    HiveTable table = new HiveTable();
    table.setDatabaseName("default_2");
    table.setTableName("customers_2");
    table.setTags(Sets.newHashSet("prereg",
        "hivetable"));

    // Note how we are getting the Hive Source here
    NavApiCient client = plugin.getClient();
    Source hiveSource = client.getHMSSource();
    table.setSourceId(hiveSource.getIdentity());

    // Write metadata
    ResultSet results = plugin.write(table);

    if (results.hasErrors()) {
      throw new RuntimeException(results.toString());
    }
  }
}
