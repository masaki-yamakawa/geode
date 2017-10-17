/**
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
package org.apache.geode.internal.cache.partitioned;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * The test creates three datastores with a partitioned region, and it holds three regions with
 * different redundant copy numbers. Every region has an entry-idle-time Expire and Eviction set up
 * to 10 entries. In this test 60 entries of data are put and all keys are acquired by OQL in each
 * VM. The result will differ if you obtain all the keys before Expire, update the last update time,
 * then obtain all the keys again by OQL. (There is no data consistency between VMs)
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class BugExpireAndEvictionDUnitTest extends JUnit4DistributedTestCase {

  private static VM vm0 = null;
  private static VM vm1 = null;
  private static VM vm2 = null;

  private static GemFireCacheImpl cache;

  private static int ENTRY_IDLE_TIMEOUT = 6;

  private static final String REGION_NAME_REDUNDANT_COPY0 =
      BugExpireAndEvictionDUnitTest.class.getSimpleName() + "_region0";
  private static final String REGION_NAME_REDUNDANT_COPY1 =
      BugExpireAndEvictionDUnitTest.class.getSimpleName() + "_region1";
  private static final String REGION_NAME_REDUNDANT_COPY2 =
      BugExpireAndEvictionDUnitTest.class.getSimpleName() + "_region2";

  @Override
  public final void postSetUp() throws Exception {
    disconnectFromDS();
    Host host = Host.getHost(0);
    vm0 = host.getVM(0); // datastore and server
    vm1 = host.getVM(1); // datastore
    vm2 = host.getVM(2); // datastore

    vm0.invoke(() -> BugExpireAndEvictionDUnitTest.createPRDatastore());
    vm1.invoke(() -> BugExpireAndEvictionDUnitTest.createPRDatastore());
    vm2.invoke(() -> BugExpireAndEvictionDUnitTest.createPRDatastore());
  }

  @Override
  public final void preTearDown() throws Exception {
    closeCache();

    vm0.invoke(() -> BugExpireAndEvictionDUnitTest.closeCache());
    vm1.invoke(() -> BugExpireAndEvictionDUnitTest.closeCache());
    vm2.invoke(() -> BugExpireAndEvictionDUnitTest.closeCache());
  }

  public static void closeCache() throws Exception {
    if (cache != null) {
      cache.close();
    }
  }

  @SuppressWarnings("deprecation")
  public static void createPRDatastore() throws Exception {
    Properties props = new Properties();
    BugExpireAndEvictionDUnitTest test = new BugExpireAndEvictionDUnitTest();
    DistributedSystem ds = test.getSystem(props);
    ds.disconnect();
    cache = (GemFireCacheImpl) CacheFactory.create(test.getSystem());

    RegionFactory<String, String> rf0 = cache.createRegionFactory(RegionShortcut.PARTITION);
    rf0.setEntryIdleTimeout(new ExpirationAttributes(ENTRY_IDLE_TIMEOUT, ExpirationAction.DESTROY))
        .setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(10, EvictionAction.LOCAL_DESTROY))
        .setPartitionAttributes(
            new PartitionAttributesFactory<String, String>().setRedundantCopies(0).create());
    rf0.create(REGION_NAME_REDUNDANT_COPY0);

    RegionFactory<String, String> rf1 = cache.createRegionFactory(RegionShortcut.PARTITION);
    rf1.setEntryIdleTimeout(new ExpirationAttributes(ENTRY_IDLE_TIMEOUT, ExpirationAction.DESTROY))
        .setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(10, EvictionAction.LOCAL_DESTROY))
        .setPartitionAttributes(
            new PartitionAttributesFactory<String, String>().setRedundantCopies(1).create());
    rf1.create(REGION_NAME_REDUNDANT_COPY1);

    RegionFactory<String, String> rf2 = cache.createRegionFactory(RegionShortcut.PARTITION);
    rf2.setEntryIdleTimeout(new ExpirationAttributes(ENTRY_IDLE_TIMEOUT, ExpirationAction.DESTROY))
        .setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(10, EvictionAction.LOCAL_DESTROY))
        .setPartitionAttributes(
            new PartitionAttributesFactory<String, String>().setRedundantCopies(2).create());
    rf2.create(REGION_NAME_REDUNDANT_COPY2);
  }

  @SuppressWarnings("unchecked")
  public static void doPuts(String regionName, Integer numOfPuts) throws Exception {
    Region<Integer, String> region = cache.getRegion(regionName);

    for (int i = 0; i < numOfPuts; i++) {
      region.put(i, "VALUE_" + String.valueOf(i));
    }
  }

  @SuppressWarnings("unchecked")
  public static void doGets(String regionName, Integer numOfGets) throws Exception {
    Region<Integer, String> region = cache.getRegion(regionName);

    for (int i = 0; i < numOfGets; i++) {
      region.get(i);
    }
  }

  @SuppressWarnings("unchecked")
  public static List<Integer> queryRegionKeys(String regionName) throws Exception {
    Cache cache = CacheFactory.getAnyInstance();
    Query query =
        cache.getQueryService().newQuery(String.format("select k from /%s.keySet k", regionName));
    SelectResults<Integer> result = SelectResults.class.cast(query.execute());
    return result.asList().stream().sorted().collect(Collectors.toList());
  }

  @Test
  public void testExpireAndEvictionPRRedundantCopy0() throws Exception {
    String regionName = REGION_NAME_REDUNDANT_COPY0;
    int numOfPuts = 60;

    vm0.invoke(() -> BugExpireAndEvictionDUnitTest.doPuts(regionName, numOfPuts));

    List<Integer> keys0 =
        vm0.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    List<Integer> keys1 =
        vm1.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    List<Integer> keys2 =
        vm2.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    assertArrayEquals(keys0.toArray(new Integer[0]), keys1.toArray(new Integer[0]));
    assertArrayEquals(keys1.toArray(new Integer[0]), keys2.toArray(new Integer[0]));

    TimeUnit.SECONDS.sleep(ENTRY_IDLE_TIMEOUT / 2);

    // update the last access time of all entries.
    vm0.invoke(() -> BugExpireAndEvictionDUnitTest.doGets(regionName, numOfPuts));

    TimeUnit.SECONDS.sleep(ENTRY_IDLE_TIMEOUT / 2 + 1);

    keys0 = vm0.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    keys1 = vm1.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    keys2 = vm2.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    assertArrayEquals(keys0.toArray(new Integer[0]), keys1.toArray(new Integer[0]));
    assertArrayEquals(keys1.toArray(new Integer[0]), keys2.toArray(new Integer[0]));
  }

  @Test
  public void testExpireAndEvictionPRRedundantCopy1() throws Exception {
    String regionName = REGION_NAME_REDUNDANT_COPY1;
    int numOfPuts = 60;

    vm0.invoke(() -> BugExpireAndEvictionDUnitTest.doPuts(regionName, numOfPuts));

    List<Integer> keys0 =
        vm0.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    List<Integer> keys1 =
        vm1.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    List<Integer> keys2 =
        vm2.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    assertArrayEquals(keys0.toArray(new Integer[0]), keys1.toArray(new Integer[0]));
    assertArrayEquals(keys1.toArray(new Integer[0]), keys2.toArray(new Integer[0]));

    TimeUnit.SECONDS.sleep(ENTRY_IDLE_TIMEOUT / 2);

    // update the last access time of all entries.
    vm0.invoke(() -> BugExpireAndEvictionDUnitTest.doGets(regionName, numOfPuts));

    TimeUnit.SECONDS.sleep(ENTRY_IDLE_TIMEOUT / 2 + 1);

    keys0 = vm0.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    keys1 = vm1.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    keys2 = vm2.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    assertArrayEquals(keys0.toArray(new Integer[0]), keys1.toArray(new Integer[0]));
    assertArrayEquals(keys1.toArray(new Integer[0]), keys2.toArray(new Integer[0]));
  }

  @Test
  public void testExpireAndEvictionPRRedundantCopy2() throws Exception {
    String regionName = REGION_NAME_REDUNDANT_COPY2;
    int numOfPuts = 60;

    vm0.invoke(() -> BugExpireAndEvictionDUnitTest.doPuts(regionName, numOfPuts));

    List<Integer> keys0 =
        vm0.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    List<Integer> keys1 =
        vm1.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    List<Integer> keys2 =
        vm2.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    assertArrayEquals(keys0.toArray(new Integer[0]), keys1.toArray(new Integer[0]));
    assertArrayEquals(keys1.toArray(new Integer[0]), keys2.toArray(new Integer[0]));

    TimeUnit.SECONDS.sleep(ENTRY_IDLE_TIMEOUT / 2);

    // update the last access time of all entries.
    vm0.invoke(() -> BugExpireAndEvictionDUnitTest.doGets(regionName, numOfPuts));

    TimeUnit.SECONDS.sleep(ENTRY_IDLE_TIMEOUT / 2 + 1);

    keys0 = vm0.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    keys1 = vm1.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    keys2 = vm2.invoke(() -> BugExpireAndEvictionDUnitTest.queryRegionKeys(regionName));
    assertArrayEquals(keys0.toArray(new Integer[0]), keys1.toArray(new Integer[0]));
    assertArrayEquals(keys1.toArray(new Integer[0]), keys2.toArray(new Integer[0]));
  }
}
