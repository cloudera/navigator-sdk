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
package com.cloudera.nav.plugin.model.relations;

import com.cloudera.nav.plugin.model.MD5IdGenerator;
import com.cloudera.nav.plugin.model.SourceType;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This class exposes methods that are used to generate identities for
 * relations.
 */
public class RelationIdGenerator {

  public String generateRelationIdentity(
      Collection<String> ep1Ids, SourceType ep1SourceType,
      Collection<String> ep2Ids, SourceType ep2SourceType,
      RelationType relType) {
    List<String> sortedEp1Ids = Lists.newArrayList(ep1Ids);
    Collections.sort(sortedEp1Ids);
    List<String> sortedEp2Ids = Lists.newArrayList(ep2Ids);
    Collections.sort(sortedEp2Ids);
    return MD5IdGenerator.generateIdentity(relType.name(),
        Joiner.on(",").join(sortedEp1Ids), ep1SourceType.name(),
        Joiner.on(",").join(sortedEp2Ids), ep2SourceType.name());
  }
}