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
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.entities.HiveColumn;
import com.google.common.collect.Sets;

/**
 * Tagging Hive columns
 *
 * Tags is an important part of business metadata. This example uses the
 * Navigator plugin to tag a Hive column as sensitive.
 * Users and applications can then the tags to trigger actions such as
 * encryption, masking, and/or restrictions to permissions.
 */
public class SetHiveTags {

  public static void main(String[] args) {

    // setup the plugin and api client
    NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile(args[0]);

    // send tags for multiple entities to Navigator
    HiveColumn column = new HiveColumn();
    column.setDatabaseName("default");
    column.setTableName("sample_07");
    column.setColumnName("code");
    column.setTags(Sets.newHashSet("HAS_SENSITIVE_FILES",
        "CONTAINS_SOME_SUPER_SECRET_STUFF"));

    NavApiCient client = plugin.getClient();
    Source hive = client.getOnlySource(SourceType.HIVE);
    column.setSourceId(hive.getIdentity());

    plugin.write(column);
  }
}
