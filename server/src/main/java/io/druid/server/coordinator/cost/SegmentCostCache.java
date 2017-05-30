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

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Comparators;
import io.druid.server.coordinator.CostBalancerStrategy;
import io.druid.timeline.DataSegment;
import org.apache.commons.math3.util.FastMath;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SegmentCostCache
{
  private static final double HALF_LIFE = 1.0; // cost function half-life in hours
  private static final double LAMBDA = Math.log(2) / HALF_LIFE;
  private static final double MILLIS_FACTOR = TimeUnit.DAYS.toMillis(1) / LAMBDA;
  private static final long LIFE_THRESHOLD = TimeUnit.DAYS.toMillis(30);
  private static final long BUCKET_INTERVAL = TimeUnit.DAYS.toMillis(30);

  private static final Comparator<DataSegment> SEGMENT_INTERVAL_COMPARATOR = (d1, d2) ->
      Comparators.intervalsByStartThenEnd().compare(
          d1.getInterval(),
          d2.getInterval()
      );

  private static final Comparator<Bucket> BUCKET_INTERVAL_COMPARATOR = (b1, b2) ->
      Comparators.intervalsByStartThenEnd().compare(
          b1.getInterval(),
          b2.getInterval()
      );

  private final List<Bucket> sortedBuckets;
  private final List<Interval> intervals;

  public SegmentCostCache(List<Bucket> sortedBuckets)
  {
    this.sortedBuckets = Preconditions.checkNotNull(sortedBuckets, "buckets should not be null");
    this.intervals = sortedBuckets.stream().map(Bucket::getInterval).collect(Collectors.toCollection(ArrayList::new));
    Preconditions.checkArgument(
        Ordering.from(BUCKET_INTERVAL_COMPARATOR).isOrdered(sortedBuckets),
        "buckets must be ordered by interval"
    );
  }

  public double cost(DataSegment segment)
  {
    double cost = 0.0;
    int index = Collections.binarySearch(intervals, segment.getInterval(), Comparators.intervalsByStartThenEnd());
    index = (index >= 0) ? index : -index - 1;

    ListIterator<Bucket> it = sortedBuckets.listIterator(index);
    while (it.hasNext()) {
      Bucket bucket = it.next();
      if (!bucket.inCalculationInterval(segment)) {
        break;
      }
      cost += bucket.cost(segment);
    }

    it = sortedBuckets.listIterator(index);
    while (it.hasPrevious()) {
      Bucket bucket = it.previous();
      if (!bucket.inCalculationInterval(segment)) {
        break;
      }
      cost += bucket.cost(segment);
    }

    return cost;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private NavigableMap<Interval, Bucket.Builder> buckets = new TreeMap<>(Comparators.intervalsByStartThenEnd());

    public void addSegment(DataSegment segment)
    {
      Bucket.Builder builder = buckets.computeIfAbsent(toBucketInterval(segment), Bucket::builder);
      builder.addSegment(segment);
    }

    public void removeSegement(DataSegment segment)
    {
      Interval interval = toBucketInterval(segment);
      Bucket.Builder builder = buckets.get(interval);
      if (builder != null) {
        builder.removeSegment(segment);
        if (builder.size() == 0) {
          buckets.remove(interval);
        }
      }
    }

    public int size()
    {
      return buckets.size();
    }

    public SegmentCostCache build()
    {
      return new SegmentCostCache(
          buckets
              .entrySet()
              .stream()
              .map(entry -> entry.getValue().build())
              .collect(Collectors.toCollection(ArrayList::new))
      );
    }

    private Interval toBucketInterval(DataSegment segment)
    {
      long start = segment.getInterval().getStartMillis() - (segment.getInterval().getStartMillis() / BUCKET_INTERVAL);
      return new Interval(start, start + BUCKET_INTERVAL);
    }
  }

  static class Bucket
  {
    private final Interval interval;
    private final ArrayList<DataSegment> sortedSegments;
    private final double[] leftSum;
    private final double[] rightSum;

    public Bucket(Interval interval, ArrayList<DataSegment> sortedSegments, double[] leftSum, double[] rightSum)
    {
      this.interval = Preconditions.checkNotNull(interval, "interval");
      this.sortedSegments = Preconditions.checkNotNull(sortedSegments, "sortedSegments");
      this.leftSum = Preconditions.checkNotNull(leftSum, "leftSum");
      this.rightSum = Preconditions.checkNotNull(rightSum, "rightSum");
      Preconditions.checkArgument(sortedSegments.size() == leftSum.length && sortedSegments.size() == rightSum.length);
      Preconditions.checkArgument(Ordering.from(SEGMENT_INTERVAL_COMPARATOR).isOrdered(sortedSegments));
    }

    public Interval getInterval()
    {
      return interval;
    }

    public boolean inCalculationInterval(DataSegment dataSegment)
    {
      return Math.abs(dataSegment.getInterval().getStartMillis() - interval.getStartMillis()) < LIFE_THRESHOLD
             && Math.abs(dataSegment.getInterval().getEndMillis() - interval.getStartMillis()) < LIFE_THRESHOLD;
    }

    public double cost(DataSegment dataSegment)
    {
      double cost = 0.0;

      double t0 = convertStart(dataSegment, interval);
      double t1 = convertEnd(dataSegment, interval);

      // avoid calculation for segments outside of LIFE_THRESHOLD
      if (!inCalculationInterval(dataSegment)) {
        return cost;
      }

      int index = Collections.binarySearch(sortedSegments, dataSegment, SEGMENT_INTERVAL_COMPARATOR);
      index = (index >= 0) ? index : -index - 1;

      // add to cost all left-overlapping segments
      int leftIndex = index - 1;
      while (leftIndex >= 0 && sortedSegments.get(leftIndex).getInterval().overlaps(dataSegment.getInterval())) {
        double start = convertStart(sortedSegments.get(leftIndex), interval);
        double end = convertEnd(sortedSegments.get(leftIndex), interval);
        cost += CostBalancerStrategy.intervalCost(end - start, t0 - start, t1 - start);
        --leftIndex;
      }
      // add left-non-overlapping segments
      if (leftIndex >= 0) {
        cost += leftSum[leftIndex] * (FastMath.exp(-t1) - FastMath.exp(-t0));
      }
      // add all right-overlapping segments
      int rightIndex = index;
      while (rightIndex < sortedSegments.size() && sortedSegments.get(rightIndex)
                                                                 .getInterval()
                                                                 .overlaps(dataSegment.getInterval())) {
        double start = convertStart(sortedSegments.get(rightIndex), interval);
        double end = convertEnd(sortedSegments.get(rightIndex), interval);
        cost += CostBalancerStrategy.intervalCost(t1 - t0, start - t0, end - t0);
        ++rightIndex;
      }
      // add right-non-overlapping segments
      if (rightIndex < sortedSegments.size()) {
        cost += rightSum[rightIndex] * (FastMath.exp(t0) - FastMath.exp(t1));
      }
      return cost;
    }

    private static double convertStart(DataSegment dataSegment, Interval interval)
    {
      return toLocalInterval(dataSegment.getInterval().getStartMillis(), interval);
    }

    private static double convertEnd(DataSegment dataSegment, Interval interval)
    {
      return toLocalInterval(dataSegment.getInterval().getEndMillis(), interval);
    }

    private static double toLocalInterval(long millis, Interval interval)
    {
      return (millis - interval.getStartMillis()) / MILLIS_FACTOR;
    }

    public static Builder builder(Interval interval)
    {
      return new Builder(interval);
    }

    static class Builder
    {
      private final Interval interval;
      private final LinkedList<DataSegment> dataSegments = new LinkedList<>();
      private final SumList leftSum = new SumList(false);
      private final SumList rightSum = new SumList(true);

      public Builder(Interval interval)
      {
        this.interval = interval;
      }

      public void addSegment(DataSegment dataSegment)
      {
        if (!interval.contains(dataSegment.getInterval().getStartMillis())) {
          throw new ISE("Failed to add segment to bucket: interval is not covered by this bucket");
        }
        int index = Collections.binarySearch(dataSegments, dataSegment, SEGMENT_INTERVAL_COMPARATOR);
        index = (index >= 0) ? index : -index - 1;

        dataSegments.add(index, dataSegment);

        double t0 = convertStart(dataSegment, interval);
        double t1 = convertEnd(dataSegment, interval);

        double leftValue = FastMath.exp(t0) - FastMath.exp(t1);
        double rightValue = FastMath.exp(-t1) - FastMath.exp(-t0);

        leftSum.add(index, leftValue);
        rightSum.add(index, rightValue);
      }

      public void removeSegment(DataSegment dataSegment)
      {
        int index = -1;
        ListIterator<DataSegment> it = dataSegments.listIterator();
        while (it.hasNext()) {
          DataSegment value = it.next();
          ++index;
          if (value.getIdentifier().equals(dataSegment.getIdentifier())) {
            it.remove();
            break;
          }
        }

        if (index < 0) {
          throw new ISE("Failed to remove segment from bucket: segment not found");
        }

        double t0 = convertStart(dataSegment, interval);
        double t1 = convertEnd(dataSegment, interval);

        double leftValue = FastMath.exp(t0) - FastMath.exp(t1);
        double rightValue = FastMath.exp(-t1) - FastMath.exp(-t0);

        leftSum.remove(index, leftValue);
        rightSum.remove(index, rightValue);
      }

      public int size()
      {
        return dataSegments.size();
      }

      public Bucket build()
      {
        return new Bucket(
            new Interval(
                interval.getStartMillis(),
                dataSegments.getLast().getInterval().getEndMillis()
            ),
            new ArrayList<>(dataSegments),
            leftSum.getElements().stream().mapToDouble(Double::doubleValue).toArray(),
            rightSum.getElements().stream().mapToDouble(Double::doubleValue).toArray()
        );
      }
    }
  }

  static class SumList
  {
    private final LinkedList<Double> elements = new LinkedList<>();
    private final boolean reverse;

    public SumList(boolean reverse)
    {
      this.reverse = reverse;
    }

    public LinkedList<Double> getElements()
    {
      return elements;
    }

    public void add(int index, double value)
    {
      if (reverse) {
        updateInternal(elements.listIterator(index), value, false);
        if (index < elements.size()) {
          value += elements.get(index);
        }
      } else {
        updateInternal(elements.listIterator(index), value, true);
        if (index > 0) {
          value += elements.get(index - 1);
        }
      }
      elements.add(index, value);
    }

    public void remove(int index, double value)
    {
      if (reverse) {
        updateInternal(elements.listIterator(index), -value, false);
      } else {
        updateInternal(elements.listIterator(index), -value, true);
      }
      elements.remove(index);
    }

    private void updateInternal(ListIterator<Double> it, double value, boolean upfront)
    {
      if (upfront) {
        while (it.hasNext()) {
          double current = it.next();
          it.set(current + value);
        }
      } else {
        while (it.hasPrevious()) {
          double current = it.previous();
          it.set(current + value);
        }
      }
    }
  }
}
