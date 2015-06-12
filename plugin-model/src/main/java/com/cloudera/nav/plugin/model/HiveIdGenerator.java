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

package com.cloudera.nav.plugin.model;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;

public class HiveIdGenerator {

  public static String generateTableId(String sourceId, String databaseName,
                                       String tableName) {
    Preconditions.checkArgument(!StringUtils.isEmpty(sourceId) &&
            !StringUtils.isEmpty(databaseName) &&
            !StringUtils.isEmpty(tableName),
        "SourceId, database name, and table name must be supplied to " +
            "generate Hive table identity");
    return MD5IdGenerator.generateIdentity(sourceId, databaseName, tableName);
  }

  public static String generateColumnId(String sourceId, String databaseName,
                                        String tableName, String columnName) {
    Preconditions.checkArgument(!StringUtils.isEmpty(sourceId) &&
            !StringUtils.isEmpty(databaseName) &&
            !StringUtils.isEmpty(tableName) &&
            !StringUtils.isEmpty(columnName),
        "SourceId, database name, table name, and column name must be " +
            "supplied to generate Hive column identity");
    return MD5IdGenerator.generateIdentity(sourceId, databaseName, tableName,
        columnName);
  }
}
