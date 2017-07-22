package com.cloudera.nav.sdk.examples.tags;

import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.entities.PigOperation;
import com.google.common.collect.Sets;

public class SetDjangoScriptTags {
  public static void main(String[] args) {

    // setup the plugin and api client
    NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile(args[0]);
    NavApiCient client = plugin.getClient();

    PigOperation pig = new PigOperation(
        "44894c17c795256cc930b44702c40a0e",
        "PigLatin:id.pig");

    pig.setTags(Sets.newHashSet("TAG_1",
        "TAG_2"));

    ResultSet results = plugin.write(pig);

    if (results.hasErrors()) {
      throw new RuntimeException(results.toString());
    }
  }
}
