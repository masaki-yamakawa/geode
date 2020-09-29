/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.connectors.jdbc;

import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;

import org.junit.Rule;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;

public class CacheXmlJdbcMappingIntegrationTest extends JdbcMappingIntegrationTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Override
  protected InternalCache createCacheAndCreateJdbcMapping(String cacheXmlTestName)
      throws Exception {
    String url = dbRule.getConnectionUrl().replaceAll("&", "&amp;");
    System.setProperty("TestDataSourceUrl", url);
    InternalCache cache =
        (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
            .set("cache-xml-file", getXmlFileForTest(cacheXmlTestName))
            .create();
    return cache;
  }

  @Override
  protected InternalCache createCacheAndCreateJdbcMappingWithWrongDataSource(
      String cacheXmlTestName) throws Exception {
    System.setProperty("TestDataSourceUrl", "jdbc:mysql://localhost/test");
    InternalCache cache =
        (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
            .set("cache-xml-file", getXmlFileForTest(cacheXmlTestName))
            .create();
    return cache;
  }

  @Override
  protected InternalCache createCacheAndCreateJdbcMappingWithWrongPdxName(String cacheXmlTestName)
      throws Exception {
    String url = dbRule.getConnectionUrl().replaceAll("&", "&amp;");
    System.setProperty("TestDataSourceUrl", url);
    InternalCache cache =
        (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0")
            .set("cache-xml-file", getXmlFileForTest(cacheXmlTestName))
            .create();
    return cache;
  }

  private String getXmlFileForTest(String testName) {
    return createTempFileFromResource(getClass(),
        getClassSimpleName() + "." + testName + ".cache.xml").getAbsolutePath();
  }

  private String getClassSimpleName() {
    return getClass().getSimpleName();
  }

  @Override
  protected String getConnectWrongDataSourceMessage() {
    return String.format("No datasource \"%s\" found when creating mapping \"%s\"",
        DATA_SOURCE_NAME, REGION_NAME);
  }

}
