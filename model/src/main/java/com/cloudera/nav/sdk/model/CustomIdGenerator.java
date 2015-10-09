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

package com.cloudera.nav.sdk.model;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;

public class CustomIdGenerator {

  public static String generateIdentity(String...args) {
    for (String s : args) {
      Preconditions.checkArgument(!StringUtils.isEmpty(s),
          "An identity component must not be null or empty");
    }
    return MD5IdGenerator.generateIdentity(args);
  }
}
