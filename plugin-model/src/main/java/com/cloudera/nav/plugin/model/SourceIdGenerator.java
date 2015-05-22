// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.model;

public class SourceIdGenerator {

  /**
   * Generate a unique identity for a Source given the name of the Source and
   * the name of the cluster it is deployed on.
   *
   * @param clusterName
   * @param name
   * @return
   */
  public static String generateSourceId(String clusterName, String name) {
    return MD5IdGenerator.generateIdentity(clusterName, name);
  }
}
