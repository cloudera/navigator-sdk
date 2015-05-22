// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.model;

import com.google.common.base.Preconditions;

/**
 * Encapsulates information about Hadoop component services from which entities
 * and relations can be derived.
 */
public class Source {

  private final String name;
  private final SourceType sourceType;
  private final String clusterName;
  private final String sourceUrl;
  private final String identity;

  /**
   * @param name
   * @param clusterName
   * @param sourceUrl
   * @param identity
   */
  public Source(String name, SourceType sourceType,
                String clusterName, String sourceUrl,
                String identity) {
    Preconditions.checkNotNull(identity);
    this.name = name;
    this.sourceType = sourceType;
    this.clusterName = clusterName;
    this.sourceUrl = sourceUrl;
    this.identity = identity;
  }

  public Source(String name, SourceType sourceType,
                String clusterName, String sourceUrl) {
    this(name, sourceType, clusterName, sourceUrl,
        SourceIdGenerator.generateSourceId(clusterName, name));
  }


  /**
   * @return the name of the Hadoop component service
   */
  public String getName() {
    return name;
  }

  /**
   * Source types are generally the canonical names of Hadoop comoponent
   * services like HDFS, HIVE, etc.
   *
   * @return the type of the metadata source
   */
  public SourceType getSourceType() {
    return sourceType;
  }

  /**
   * The name of the cluster assigned/set in Cloudera Manager
   *
   * @return the name of the cluster
   */
  public String getClusterName() {
    return clusterName;
  }

  /**
   * Source url's are generally the rpc addresses (host and port) of the
   * service
   *
   * @return the url for the Hadoop component service
   */
  public String getSourceUrl() {
    return sourceUrl;
  }

  /**
   * @return the id of this Source
   */
  public String getIdentity() {
    return identity;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Source source = (Source) o;

    return identity.equals(source.identity);
  }

  @Override
  public int hashCode() {
    return identity.hashCode();
  }
}
