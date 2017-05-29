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

package io.druid.server.coordinator;

import io.druid.server.coordinator.cost.SegmentCostCache;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class CachingCostBalancerStrategyBenchmark
{
  private static final int NUMBER_OF_SEGMENTS = 1000;

  private DateTime referenceTime = new DateTime("2014-01-01T00:00:00");
  private List<DataSegment> segments = new ArrayList<>();
  private DataSegment segment;

  private SegmentCostCache.Bucket bucket;

  @Setup
  public void createSegments()
  {
    Random random = new Random();
    SegmentCostCache.Bucket.Builder prototype = SegmentCostCache.Bucket.builder(new Interval(referenceTime.minusHours(1), referenceTime.plusHours(30 * 25)));
    for (int i = 0; i < NUMBER_OF_SEGMENTS; ++i) {
      DataSegment segment = createSegment(random.nextInt(30 * 24));
      segments.add(segment);
      prototype.addSegment(segment);
    }
    bucket = prototype.build();
    segment = createSegment(random.nextInt(30*24));
  }



  private DataSegment createSegment(int shift)
  {
    return new DataSegment(
        "dataSource",
        new Interval(referenceTime.plusHours(shift), referenceTime.plusHours(shift).plusHours(1)),
        "version",
        Collections.<String, Object>emptyMap(),
        Collections.<String>emptyList(),
        Collections.<String>emptyList(),
        null,
        0,
        100
    );
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(1)
  public double measureCostStrategy() throws InterruptedException
  {
    return CostBalancerStrategy.computeJointSegmentsCost(segment, segments);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(1)
  public double measureCachingCostStrategy() throws InterruptedException
  {
    return bucket.cost(segment);
  }

}
