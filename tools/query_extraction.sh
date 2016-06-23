#!/bin/bash

mvn -f ../examples/pom.xml -X exec:java -Dexec.mainClass="com.cloudera.nav.sdk.examples.extraction.QueryExtraction" -Dexec.args="$1"
