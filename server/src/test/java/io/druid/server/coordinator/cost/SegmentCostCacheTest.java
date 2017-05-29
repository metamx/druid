/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.cost;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class SegmentCostCacheTest
{

  private static final String DATA_SOURCE = "dataSource";
  private static final DateTime REFERENCE_TIME = new DateTime("2014-01-01T00:00:00");
  private static final double EPSILON = 0.0000001;

  private SegmentCostCache.Bucket bucketMock;

  @Before
  public void setUp() throws Exception
  {
    bucketMock = EasyMock.createMock(SegmentCostCache.Bucket.class);
  }

  @Test
  public void segmentCacheTest()
  {
    SegmentCostCache.Builder cacheBuilder = SegmentCostCache.builder();
    cacheBuilder.addSegment(createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100));
    SegmentCostCache cache = cacheBuilder.build();
    assertEquals(7.8735899489011E-4,
                 cache.cost(createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, -2), 100)),
                 EPSILON
    );
  }
  
  @Test
  public void twoSegmentsCostTest()
  {
    DataSegment segmentA = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100);
    DataSegment segmentB = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, -2), 100);

    SegmentCostCache.Bucket.Builder prototype = SegmentCostCache.Bucket.builder(new Interval(
        REFERENCE_TIME.minusHours(5),
        REFERENCE_TIME.plusHours(5)
    ));

    prototype.addSegment(segmentA);
    SegmentCostCache.Bucket bucket = prototype.build();

    double segmentCost = bucket.cost(segmentB);
    assertEquals(7.8735899489011E-4, segmentCost, EPSILON);
  }

  @Test
  public void sameSegmentCostTest()
  {
    DataSegment segmentA = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100);
    DataSegment segmentB = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100);

    SegmentCostCache.Bucket.Builder prototype = SegmentCostCache.Bucket.builder(new Interval(
        REFERENCE_TIME.minusHours(5),
        REFERENCE_TIME.plusHours(5)
    ));

    prototype.addSegment(segmentA);
    SegmentCostCache.Bucket bucket = prototype.build();

    double segmentCost = bucket.cost(segmentB);
    assertEquals(8.26147353873985E-4, segmentCost, EPSILON);
  }

  @Test
  public void multipleSegmentsCostTest()
  {
    DataSegment segmentA = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, -2), 100);
    DataSegment segmentB = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 0), 100);
    DataSegment segmentC = createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, 2), 100);

    SegmentCostCache.Bucket.Builder prototype = SegmentCostCache.Bucket.builder(new Interval(
        REFERENCE_TIME.minusHours(5),
        REFERENCE_TIME.plusHours(5)
    ));

    prototype.addSegment(segmentA);
    prototype.addSegment(segmentC);
    SegmentCostCache.Bucket bucket = prototype.build();

    double segmentCost = bucket.cost(segmentB);

    assertEquals(0.001574717989780039, segmentCost, EPSILON);
  }

  @Test
  public void randomSegmentsCostTest()
  {
    List<DataSegment> dataSegments = new ArrayList<>(1000);
    Random random = new Random(1);
    for (int i=0; i < 1000; ++i) {
      dataSegments.add(createSegment(DATA_SOURCE, shifted1HInterval(REFERENCE_TIME, random.nextInt(20)), 100));
    }

    DataSegment referenceSegment = createSegment("ANOTHER_DATA_SOURCE", shifted1HInterval(REFERENCE_TIME, 5), 100);

    SegmentCostCache.Bucket.Builder prototype = SegmentCostCache.Bucket.builder(new Interval(
        REFERENCE_TIME.minusHours(1),
        REFERENCE_TIME.plusHours(25)
    ));
    dataSegments.forEach(prototype::addSegment);
    SegmentCostCache.Bucket bucket = prototype.build();

    double cost = bucket.cost(referenceSegment);
    assertEquals(0.7065117101966677, cost, EPSILON);
  }


  @Test
  public void sumListTest() throws Exception
  {
    SegmentCostCache.SumList sumList = new SegmentCostCache.SumList(false);
    sumList.add(0, 3.0);
    sumList.add(1, 4.0);
    sumList.add(2, 1.0);
    sumList.add(1, 1.0);
    assertEquals(Lists.newLinkedList(Lists.newArrayList(3.0, 4.0, 8.0, 9.0)), sumList.getElements());
    sumList.remove(1, 1.0);
    assertEquals(Lists.newLinkedList(Lists.newArrayList(3.0, 7.0, 8.0)), sumList.getElements());
    sumList.remove(0, 3.0);
    assertEquals(Lists.newLinkedList(Lists.newArrayList(4.0, 5.0)), sumList.getElements());
  }

  @Test
  public void reverseSumListTest() throws Exception
  {
    SegmentCostCache.SumList sumList = new SegmentCostCache.SumList(true);
    sumList.add(0, 3.0);
    sumList.add(1, 4.0);
    sumList.add(2, 1.0);
    sumList.add(1, 1.0);
    assertEquals(Lists.newLinkedList(Lists.newArrayList(9.0, 6.0, 5.0, 1.0)), sumList.getElements());
    sumList.remove(1, 1.0);
    assertEquals(Lists.newLinkedList(Lists.newArrayList(8.0, 5.0, 1.0)), sumList.getElements());
    sumList.remove(0, 3.0);
    assertEquals(Lists.newLinkedList(Lists.newArrayList(5.0, 1.0)), sumList.getElements());
  }

  public static Interval shifted1HInterval(DateTime REFERENCE_TIME, int shiftInHours)
  {
    return new Interval(
        REFERENCE_TIME.plusHours(shiftInHours),
        REFERENCE_TIME.plusHours(shiftInHours + 1)
    );
  }

  public static DataSegment createSegment(String dataSource, Interval interval, long size)
  {
    return new DataSegment(
        dataSource,
        interval,
        "version",
        Maps.<String, Object>newConcurrentMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        null,
        0,
        size
    );
  }
}
