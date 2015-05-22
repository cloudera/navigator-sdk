package com.cloudera.nav.plugin.client.examples.tags;

import com.cloudera.nav.plugin.client.NavApiCient;
import com.cloudera.nav.plugin.client.NavigatorPlugin;
import com.cloudera.nav.plugin.client.PluginConfigurationFactory;
import com.cloudera.nav.plugin.client.PluginConfigurations;
import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;
import com.cloudera.nav.plugin.model.entities.HdfsEntity;
import com.google.common.collect.Sets;

/**
 * A more convenient/robust way to set tags on HDFS entities
 */
public class SetHdfsFileTags {

  public static void main(String[] args) {

    // setup the plugin and api client
    String configFilePath = args[0];
    PluginConfigurations config = (new PluginConfigurationFactory())
        .readConfigurations(configFilePath);
    NavigatorPlugin plugin = new NavigatorPlugin(config);
    NavApiCient client = new NavApiCient(config);
    Source hdfs = client.getOnlySource(SourceType.HDFS);

    // send tags for multiple entities to Navigator
    HdfsEntity dir = new HdfsEntity();
    dir.setSourceId(hdfs.getIdentity());
    dir.setFileSystemPath("/user/hdfs");
    dir.setTags(Sets.newHashSet("HAS_SENSITIVE_FILES",
        "CONTAINS_SOME_SUPER_SECRET_STUFF"));

    plugin.write(dir);
  }
}
