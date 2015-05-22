// (c) Copyright 2015 Cloudera, Inc. All rights reserved.
package com.cloudera.nav.plugin.model;

import com.cloudera.nav.plugin.model.entities.Entity;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;


/**
 * Factory class for generating unique identities for one or more strings
 */
public class MD5IdGenerator {

  @VisibleForTesting
  public static Pattern MD5_PATTERN = Pattern.compile("[a-fA-F0-9]{32}");

  /**
   * Generates identity by creating MD5 hash on combination of all the specified
   * arg in order.
   */
  public static String generateIdentity(String... args) {
    MD5IdGenerator h = new MD5IdGenerator();
    h.update(args);
    return h.getIdentity();
  }

  public static boolean isValidId(String id) {
    return !StringUtils.isEmpty(id) && MD5_PATTERN.matcher(id).matches();
  }

  private final Hasher hasher = Hashing.md5().newHasher();

  /**
   * Update the MD5 Hasher with given string followed by the entity separator
   * @param arg
   */
  public void update(String arg) {
    hasher.putString(arg);
    hasher.putString(Entity.ID_SEPARATOR);
  }

  /**
   * Update the MD5 Hasher with given strings followed by the entity separator
   * @param args
   */
  public void update(String... args) {
    int i = 0;
    for (String arg : args) {
      if (arg != null) {
        hasher.putString(arg);
      }
      if (i < args.length - 1) {
        hasher.putString(Entity.ID_SEPARATOR);
      }
      i++;
    }
  }

  public String getIdentity() {
    return hasher.hash().toString();
  }

}
