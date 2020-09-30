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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.connectors.jdbc.internal.cli.CreateMappingFunction;
import org.apache.geode.connectors.jdbc.internal.cli.CreateMappingPreconditionCheckFunction;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.management.internal.cli.commands.CreateJndiBindingCommand;
import org.apache.geode.management.internal.cli.functions.CreateJndiBindingFunction;
import org.apache.geode.management.internal.cli.functions.CreateRegionFunctionArgs;
import org.apache.geode.management.internal.cli.functions.RegionCreateFunction;
import org.apache.geode.management.internal.configuration.converters.RegionConverter;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.util.ManagementUtils;

public class GfshJdbcMappingIntegrationTest extends JdbcMappingIntegrationTest {

  @Override
  protected InternalCache createCacheAndCreateJdbcMapping(String cacheXmlTestName)
      throws Exception {
    InternalCache cache =
        (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0").create();
    Set<DistributedMember> targetMembers = findMembers(cache, null, null);

    CliFunctionResult createRegionFuncResult = executeCreateRegionFunction(targetMembers);
    System.out.println("createRegionFuncResult=" + createRegionFuncResult);

    CliFunctionResult createDataStoreFuncResult =
        executeCreateJndiBindingFunction(targetMembers, dbRule.getConnectionUrl());
    System.out.println("createDataStoreFuncArgs=" + createDataStoreFuncResult);

    CliFunctionResult createMappingFuncResult =
        executeCreateMappingFunction(Employee.class.getName(), targetMembers);
    System.out.println("createMappingFuncResult=" + createMappingFuncResult);

    return cache;
  }

  @Override
  protected InternalCache createCacheAndCreateJdbcMappingWithWrongDataSource(
      String cacheXmlTestName) throws Exception {
    InternalCache cache =
        (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0").create();
    Set<DistributedMember> targetMembers = findMembers(cache, null, null);

    CliFunctionResult createRegionFuncResult = executeCreateRegionFunction(targetMembers);
    System.out.println("createRegionFuncResult=" + createRegionFuncResult);

    CliFunctionResult createDataStoreFuncResult =
        executeCreateJndiBindingFunction(targetMembers, "jdbc:mysql://localhost/test");
    System.out.println("createDataStoreFuncArgs=" + createDataStoreFuncResult);

    CliFunctionResult createMappingFuncResult =
        executeCreateMappingFunction(Employee.class.getName(), targetMembers);
    System.out.println("createMappingFuncResult=" + createMappingFuncResult);

    return cache;
  }

  @Override
  protected InternalCache createCacheAndCreateJdbcMappingWithWrongPdxName(String cacheXmlTestName)
      throws Exception {
    InternalCache cache =
        (InternalCache) new CacheFactory().set("locators", "").set("mcast-port", "0").create();
    Set<DistributedMember> targetMembers = findMembers(cache, null, null);

    CliFunctionResult createRegionFuncResult = executeCreateRegionFunction(targetMembers);
    System.out.println("createRegionFuncResult=" + createRegionFuncResult);

    CliFunctionResult createDataStoreFuncResult =
        executeCreateJndiBindingFunction(targetMembers, dbRule.getConnectionUrl());
    System.out.println("createDataStoreFuncArgs=" + createDataStoreFuncResult);

    CliFunctionResult createMappingFuncResult =
        executeCreateMappingFunction("org.apache.geode.connectors.jdbc.NoPdx", targetMembers);
    System.out.println("createMappingFuncResult=" + createMappingFuncResult);

    return cache;
  }

  private Set<DistributedMember> findMembers(InternalCache cache, String[] groups,
      String[] members) {
    return ManagementUtils.findMembers(groups, members, cache);
  }

  private CliFunctionResult executeFunction(Function<?> function, Object args,
      Set<DistributedMember> targetMembers) {
    ResultCollector<?, ?> rc = ManagementUtils.executeFunction(function, args, targetMembers);
    List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());
    return results.size() > 0 ? results.get(0) : null;
  }

  private CliFunctionResult executeCreateRegionFunction(Set<DistributedMember> targetMembers) {
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName(REGION_NAME);
    regionConfig.setType(RegionType.REPLICATE);
    regionConfig.setRegionAttributes(
        new RegionConverter().createRegionAttributesByType(RegionType.REPLICATE.name()));
    CreateRegionFunctionArgs createRegionFuncArgs =
        new CreateRegionFunctionArgs(REGION_NAME, regionConfig, false);
    CliFunctionResult createRegionFuncResult =
        executeFunction(RegionCreateFunction.INSTANCE, createRegionFuncArgs, targetMembers);
    return createRegionFuncResult;
  }

  private CliFunctionResult executeCreateJndiBindingFunction(Set<DistributedMember> targetMembers,
      String connectionUrl) {
    JndiBindingsType.JndiBinding jndiConfig = new JndiBindingsType.JndiBinding();
    jndiConfig.setConnectionUrl(connectionUrl);
    jndiConfig.setJndiName(DATA_SOURCE_NAME);
    jndiConfig.setType(CreateJndiBindingCommand.DATASOURCE_TYPE.SIMPLE.getType());
    Object[] createDataStoreFuncArgs = new Object[] {jndiConfig, true};
    CliFunctionResult createDataStoreFuncResult =
        executeFunction(new CreateJndiBindingFunction(), createDataStoreFuncArgs, targetMembers);
    return createDataStoreFuncResult;
  }

  private CliFunctionResult executeCreateMappingFunction(String pdxClassName,
      Set<DistributedMember> targetMembers) throws Exception {
    RegionMapping mapping = new RegionMapping(REGION_NAME, pdxClassName,
        REGION_TABLE_NAME, DATA_SOURCE_NAME, "id", null, null);
    Object[] createMappingPreconditionCheckFuncArgs = new Object[] {mapping, null, null};
    CliFunctionResult createMappingPreconditionCheckFuncResult = executeFunction(
        new CreateMappingPreconditionCheckFunction(), createMappingPreconditionCheckFuncArgs,
        Collections.singleton(targetMembers.iterator().next()));
    System.out.println(
        "createMappingPreconditionCheckFuncResult=" + createMappingPreconditionCheckFuncResult);

    if (createMappingPreconditionCheckFuncResult.isSuccessful()) {
      Object[] preconditionOutput =
          (Object[]) createMappingPreconditionCheckFuncResult.getResultObject();
      String computedIds = (String) preconditionOutput[0];
      if (computedIds != null) {
        mapping.setIds(computedIds);
      }
      @SuppressWarnings("unchecked")
      List<FieldMapping> fieldMappings = (ArrayList<FieldMapping>) preconditionOutput[1];
      for (FieldMapping fieldMapping : fieldMappings) {
        mapping.addFieldMapping(fieldMapping);
      }
    } else {
      if (createMappingPreconditionCheckFuncResult.getResultObject() instanceof Exception) {
        throw (Exception) createMappingPreconditionCheckFuncResult.getResultObject();
      }
      throw new RuntimeException();
    }

    Object[] createMappingFuncArgs = new Object[] {mapping, true};
    Constructor<CreateMappingFunction> constructor =
        CreateMappingFunction.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    CreateMappingFunction createMappingFunction = constructor.newInstance();
    CliFunctionResult createMappingFuncResult =
        executeFunction(createMappingFunction, createMappingFuncArgs, targetMembers);
    return createMappingFuncResult;
  }

  @Override
  protected String getConnectWrongDataSourceMessage() {
    return String.format(
        "JDBC data-source named \"%s\" not found. Create it with gfsh 'create data-source --pooled --name=%s'.",
        DATA_SOURCE_NAME, DATA_SOURCE_NAME);
  }

  @Override
  protected Class<?> getPdxNotExistsExceptionClass() {
    return JdbcConnectorException.class;
  }

  @Override
  protected String getPdxNotExistsMessage() {
    return "The pdx class \"org.apache.geode.connectors.jdbc.NoPdx\" could not be loaded because: java.lang.ClassNotFoundException: org.apache.geode.connectors.jdbc.NoPdx";
  }

  @Override
  public void mappingFailureWhenFieldMappingAndTableMetaDataUnMatch() throws Exception {}

  @Override
  public void mappingSuccessWhenPdxFieldAndTableMetaDataUnMatchButFieldMappingMatch()
      throws Exception {}
}