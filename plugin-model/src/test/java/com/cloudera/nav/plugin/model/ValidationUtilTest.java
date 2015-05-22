package com.cloudera.nav.plugin.model;

import com.cloudera.nav.plugin.model.annotations.MProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collection;

import org.junit.*;

public class ValidationUtilTest {

  private static class MetadataClass {

    @MProperty(required=true)
    public Collection<String> getColl() {
      return ImmutableList.of("foo", "bar");
    }

    @MProperty(required=true)
    public String getStr() {
      return "foo";
    }

    @MProperty(required=true)
    public int getInt() {
      return 5;
    }
  }

  private static class BadColl extends MetadataClass {
    @MProperty(required = true)
    public Collection<String> getEmptyColl() {
      return Lists.newArrayList();
    }
  }

  private static class BadString extends MetadataClass {
    @MProperty(required = true)
    public String getEmpty() {
      return "";
    }
  }

  private static class BadObj extends MetadataClass {
    @MProperty(required = true)
    public Long getNull() {
      return null;
    }
  }

  @Test
  public void testBasic() {
    MetadataClass mclassObj = new MetadataClass();
    ValidationUtil util = new ValidationUtil();
    util.validateRequiredMProperties(mclassObj);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadColl() {
    BadColl mclassObj = new BadColl();
    ValidationUtil util = new ValidationUtil();
    util.validateRequiredMProperties(mclassObj);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadStr() {
    BadString mclassObj = new BadString();
    ValidationUtil util = new ValidationUtil();
    util.validateRequiredMProperties(mclassObj);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadObj() {
    BadObj mclassObj = new BadObj();
    ValidationUtil util = new ValidationUtil();
    util.validateRequiredMProperties(mclassObj);
  }

}
