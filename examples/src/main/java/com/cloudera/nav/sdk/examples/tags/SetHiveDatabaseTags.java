// (c) Copyright 2017 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.sdk.examples.tags;

import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.HiveDatabase;
import com.google.common.collect.Sets;

public class SetHiveDatabaseTags {

  public static void main(String[] args) {

    // setup the plugin and api client
    NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile(args[0]);

    // send tags for multiple entities to Navigator
    HiveDatabase database = new HiveDatabase();
    database.setDatabaseName("nav_policy_db2");
    database.setTags(Sets.newHashSet("AADARSH1",
        "JAJODIA1"));

    // Note how we are getting the Hive Source here
    NavApiCient client = plugin.getClient();
    Source hiveSource = client.getHMSSource();
    database.setSourceId(hiveSource.getIdentity());

    // Write metadata\
    ResultSet results = plugin.write(database);

    if (results.hasErrors()) {
      throw new RuntimeException(results.toString());
    }
  }
}
