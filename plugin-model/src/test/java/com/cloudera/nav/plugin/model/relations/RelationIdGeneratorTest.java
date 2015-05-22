package com.cloudera.nav.plugin.model.relations;

import static org.junit.Assert.*;

import com.cloudera.nav.plugin.model.MD5IdGenerator;
import com.cloudera.nav.plugin.model.SourceType;
import com.google.common.collect.ImmutableList;

import org.junit.*;

public class RelationIdGeneratorTest {

  RelationIdGenerator idGenerator;

  @Before
  public void setUp() {
    idGenerator = new RelationIdGenerator();
  }

  @Test
  public void testGenerateRelationIdentity() {
    String id = idGenerator.generateRelationIdentity(
        ImmutableList.of("ep1", "ep11"), SourceType.PLUGIN,
        ImmutableList.of("ep2", "ep21"), SourceType.PIG,
        RelationType.DATA_FLOW);
    assertEquals(MD5IdGenerator.generateIdentity(RelationType.DATA_FLOW.name(),
        "ep1,ep11", SourceType.PLUGIN.name(),
        "ep2,ep21", SourceType.PIG.name()), id);
  }

  @Test
  public void testSorted() {
    String result = idGenerator.generateRelationIdentity(
        ImmutableList.of("ep11", "ep1"), SourceType.PLUGIN,
        ImmutableList.of("ep21", "ep2"), SourceType.PIG,
        RelationType.DATA_FLOW);
    String expected = idGenerator.generateRelationIdentity(
        ImmutableList.of("ep1", "ep11"), SourceType.PLUGIN,
        ImmutableList.of("ep2", "ep21"), SourceType.PIG,
        RelationType.DATA_FLOW);
    assertEquals(expected, result);
  }
}
