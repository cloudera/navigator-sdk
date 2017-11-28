// (c) Copyright 2017 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.sdk.examples.tags;

import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.HiveColumn;
import com.google.common.collect.Sets;

public class SetHiveColumnsTags {

  public static void main(String[] args) {

    // setup the plugin and api client
    NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile(args[0]);

    // send tags for multiple entities to Navigator
    HiveColumn column = new HiveColumn();
    column.setDatabaseName("default_2");
    column.setTableName("customers_2");
    column.setColumnName("new_column");
    column.setTags(Sets.newHashSet("COL3",
        "COL"));

    NavApiCient client = plugin.getClient();
    Source hiveSource = client.getHMSSource();
    column.setSourceId(hiveSource.getIdentity());

    // Write metadata\
    ResultSet results = plugin.write(column);

    if (results.hasErrors()) {
      throw new RuntimeException(results.toString());
    }
  }
}
