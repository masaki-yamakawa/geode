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
package org.apache.geode.cache.query.internal;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

public class RestrictedMethodInvocationAuthorizer implements MethodInvocationAuthorizer {
  private static final Logger logger = LogService.getLogger();

  public static String ADD_WHITELISTED_METHOD_CLASS = System.getProperty(
      DistributionConfig.GEMFIRE_PREFIX + "QueryService.AdditionalWhiteListedMethodClasses");

  public static final String UNAUTHORIZED_STRING = "Unauthorized access to method: ";

  private SecurityService securityService;

  // List of methods that can be invoked by
  private final Map<Class<?>, Set<String>> whiteListedMethods;


  public RestrictedMethodInvocationAuthorizer(SecurityService securityService) {
    this.securityService = securityService;
    whiteListedMethods = createWhiteList();
  }

  private Map<Class<?>, Set<String>> createWhiteList() {
    Map<Class<?>, Set<String>> whiteListMap = new HashMap<>();
    Set<String> objectAllowedMethods = new HashSet<>();
    objectAllowedMethods.add("toString");
    objectAllowedMethods.add("equals");
    objectAllowedMethods.add("compareTo");
    whiteListMap.put(Object.class, objectAllowedMethods);

    Set<String> booleanAllowedMethods = new HashSet<>();
    booleanAllowedMethods.add("booleanValue");
    whiteListMap.put(Boolean.class, booleanAllowedMethods);

    Set<String> numericAllowedMethods = new HashSet<>();
    numericAllowedMethods.add("byteValue");
    numericAllowedMethods.add("intValue");
    numericAllowedMethods.add("doubleValue");
    numericAllowedMethods.add("floatValue");
    numericAllowedMethods.add("longValue");
    numericAllowedMethods.add("shortValue");
    whiteListMap.put(Number.class, numericAllowedMethods);

    Set<String> mapAllowedMethods = new HashSet<>();
    mapAllowedMethods.add("get");
    mapAllowedMethods.add("entrySet");
    mapAllowedMethods.add("keySet");
    mapAllowedMethods.add("values");
    mapAllowedMethods.add("getEntries");
    mapAllowedMethods.add("getValues");
    mapAllowedMethods.add("containsKey");
    whiteListMap.put(Map.class, mapAllowedMethods);

    Set<String> collectionAllowedMethods = new HashSet<>();
    collectionAllowedMethods.add("get");
    collectionAllowedMethods.add("entrySet");
    collectionAllowedMethods.add("keySet");
    collectionAllowedMethods.add("values");
    collectionAllowedMethods.add("getEntries");
    collectionAllowedMethods.add("getValues");
    collectionAllowedMethods.add("containsKey");
    whiteListMap.put(Collection.class, collectionAllowedMethods);

    Set<String> mapEntryAllowedMethods = new HashSet<>();
    mapEntryAllowedMethods.add("getKey");
    mapEntryAllowedMethods.add("getValue");
    whiteListMap.put(Map.Entry.class, mapEntryAllowedMethods);

    Set<String> dateAllowedMethods = new HashSet<>();
    dateAllowedMethods.add("after");
    dateAllowedMethods.add("before");
    dateAllowedMethods.add("getNanos");
    dateAllowedMethods.add("getTime");
    whiteListMap.put(Date.class, dateAllowedMethods);

    Set<String> stringAllowedMethods = new HashSet<>();
    stringAllowedMethods.add("charAt");
    stringAllowedMethods.add("codePointAt");
    stringAllowedMethods.add("codePointBefore");
    stringAllowedMethods.add("codePointCount");
    stringAllowedMethods.add("compareToIgnoreCase");
    stringAllowedMethods.add("concat");
    stringAllowedMethods.add("contains");
    stringAllowedMethods.add("contentEquals");
    stringAllowedMethods.add("endsWith");
    stringAllowedMethods.add("equalsIgnoreCase");
    stringAllowedMethods.add("getBytes");
    stringAllowedMethods.add("hashCode");
    stringAllowedMethods.add("indexOf");
    stringAllowedMethods.add("intern");
    stringAllowedMethods.add("isEmpty");
    stringAllowedMethods.add("lastIndexOf");
    stringAllowedMethods.add("length");
    stringAllowedMethods.add("matches");
    stringAllowedMethods.add("offsetByCodePoints");
    stringAllowedMethods.add("regionMatches");
    stringAllowedMethods.add("replace");
    stringAllowedMethods.add("replaceAll");
    stringAllowedMethods.add("replaceFirst");
    stringAllowedMethods.add("split");
    stringAllowedMethods.add("startsWith");
    stringAllowedMethods.add("substring");
    stringAllowedMethods.add("toCharArray");
    stringAllowedMethods.add("toLowerCase");
    stringAllowedMethods.add("toUpperCase");
    stringAllowedMethods.add("trim");
    whiteListMap.put(String.class, stringAllowedMethods);

    if (ADD_WHITELISTED_METHOD_CLASS != null && ADD_WHITELISTED_METHOD_CLASS.length() > 0) {
      whiteListMap.putAll(createAdditionalWhitelisted());
    }

    return whiteListMap;
  }

  private Map<Class<?>, Set<String>> createAdditionalWhitelisted() {
    Map<Class<?>, Set<String>> whiteListMap = new HashMap<>();
    // TODO allow wild card. for example com.aaa.bbb.*
    String[] classes = ADD_WHITELISTED_METHOD_CLASS.split(",");
    for (String classStr : classes) {
      try {
        Class<?> clazz = Class.forName(classStr.trim());
        Set<String> addClassAllowedMethods = new HashSet<>();
        // TODO restrict methods
        for (Method method : clazz.getDeclaredMethods()) {
          addClassAllowedMethods.add(method.getName());
        }
        whiteListMap.put(clazz, addClassAllowedMethods);
      } catch (ClassNotFoundException e) {
        // TODO output warn log
        logger.warn("The specified class in the system property does not exist. class="
            + ADD_WHITELISTED_METHOD_CLASS);
      }
    }
    return whiteListMap;
  }

  boolean isWhitelisted(Method method) {
    String methodName = method.getName();

    // TODO cahced methods
    for (Map.Entry<Class<?>, Set<String>> whiteListedClass : whiteListedMethods.entrySet()) {
      if (whiteListedClass.getKey().isAssignableFrom(method.getDeclaringClass())) {
        boolean result = whiteListedClass.getValue().contains(methodName);
        if (result) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void authorizeMethodInvocation(Method method, Object target) {
    if (!isWhitelisted(method)) {
      throw new NotAuthorizedException(UNAUTHORIZED_STRING + method.getName());
    }
    authorizeRegionAccess(securityService, target);
  }

  private void authorizeRegionAccess(SecurityService securityService, Object target) {
    if (target instanceof Region) {
      String regionName = ((Region<?, ?>) target).getName();
      securityService.authorize(ResourcePermission.Resource.DATA, ResourcePermission.Operation.READ,
          regionName);
    }
  }
}
