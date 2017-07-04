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
import io.druid.java.util.common.granularity.DurationGranularity;
import io.druid.java.util.common.guava.Comparators;
import io.druid.server.coordinator.CostBalancerStrategy;
import io.druid.timeline.DataSegment;
import org.apache.commons.math3.util.FastMath;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ListIterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SegmentsCostCache
{
  private static final double HALF_LIFE = 1.0; // cost function half-life in days
  private static final double LAMBDA = Math.log(2) / HALF_LIFE;
  private static final double MILLIS_FACTOR = TimeUnit.DAYS.toMillis(1) / LAMBDA;
  private static final long LIFE_THRESHOLD = TimeUnit.DAYS.toMillis(30);
  private static final long BUCKET_INTERVAL = TimeUnit.DAYS.toMillis(30);

  private static final Comparator<DataSegment> SEGMENT_INTERVAL_COMPARATOR =
      Comparator.comparing(DataSegment::getInterval, Comparators.intervalsByStartThenEnd());

  private static final Comparator<Bucket> BUCKET_INTERVAL_COMPARATOR =
      Comparator.comparing(Bucket::getInterval, Comparators.intervalsByStartThenEnd());

  private static final Ordering<DataSegment> SEGMENT_ORDERING = Ordering.from(SEGMENT_INTERVAL_COMPARATOR);
  private static final Ordering<Bucket> BUCKET_ORDERING = Ordering.from(BUCKET_INTERVAL_COMPARATOR);

  private static final DurationGranularity BUCKET_GRANULARITY = new DurationGranularity(BUCKET_INTERVAL, 0);

  private final ArrayList<Bucket> sortedBuckets;
  private final ArrayList<Interval> intervals;

  public SegmentsCostCache(ArrayList<Bucket> sortedBuckets)
  {
    this.sortedBuckets = Preconditions.checkNotNull(sortedBuckets, "buckets should not be null");
    this.intervals = sortedBuckets.stream().map(Bucket::getInterval).collect(Collectors.toCollection(ArrayList::new));
    Preconditions.checkArgument(
        BUCKET_ORDERING.isOrdered(sortedBuckets),
        "buckets must be ordered by interval"
    );
  }

  public double cost(DataSegment segment)
  {
    double cost = 0.0;
    int index = Collections.binarySearch(intervals, segment.getInterval(), Comparators.intervalsByStartThenEnd());
    index = (index >= 0) ? index : -index - 1;

    for (ListIterator<Bucket> it = sortedBuckets.listIterator(index); it.hasNext(); ) {
      Bucket bucket = it.next();
      if (!bucket.inCalculationInterval(segment)) {
        break;
      }
      cost += bucket.cost(segment);
    }

    for (ListIterator<Bucket> it = sortedBuckets.listIterator(index); it.hasPrevious(); ) {
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

    public Builder addSegment(DataSegment segment)
    {
      Bucket.Builder builder = buckets.computeIfAbsent(getBucketInterval(segment), Bucket::builder);
      builder.addSegment(segment);
      return this;
    }

    public Builder removeSegement(DataSegment segment)
    {
      Interval interval = getBucketInterval(segment);
      buckets.computeIfPresent(
          interval,
          (i, builder) -> builder.removeSegment(segment).isEmpty() ? null : builder
      );
      return this;
    }

    public boolean isEmpty()
    {
      return buckets.isEmpty();
    }

    public SegmentsCostCache build()
    {
      return new SegmentsCostCache(
          buckets
              .entrySet()
              .stream()
              .map(entry -> entry.getValue().build())
              .collect(Collectors.toCollection(ArrayList::new))
      );
    }

    private Interval getBucketInterval(DataSegment segment)
    {
      return BUCKET_GRANULARITY.bucket(segment.getInterval().getStart());
    }
  }

  static class Bucket
  {
    private final Interval interval;
    private final Interval calculationInterval;
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
      Preconditions.checkArgument(SEGMENT_ORDERING.isOrdered(sortedSegments));
      this.calculationInterval = new Interval(
          interval.getStartMillis() - LIFE_THRESHOLD,
          interval.getEndMillis() + LIFE_THRESHOLD
      );
    }

    public Interval getInterval()
    {
      return interval;
    }

    public boolean inCalculationInterval(DataSegment dataSegment)
    {
      return calculationInterval.contains(dataSegment.getInterval());
    }

    public double cost(DataSegment dataSegment)
    {
      double cost = 0.0;

      double t0 = convertStart(dataSegment, interval);
      double t1 = convertEnd(dataSegment, interval);

      // avoid calculation for segments outside of LIFE_THRESHOLD
      if (!inCalculationInterval(dataSegment)) {
        throw new ISE("Segment is not within calculation interval");
      }

      int index = Collections.binarySearch(sortedSegments, dataSegment, SEGMENT_INTERVAL_COMPARATOR);
      index = (index >= 0) ? index : -index - 1;

      // add to cost all left-overlapping segments
      int leftIndex = index - 1;
      while (leftIndex >= 0
             && sortedSegments.get(leftIndex).getInterval().overlaps(dataSegment.getInterval())) {
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
      while (rightIndex < sortedSegments.size() &&
             sortedSegments.get(rightIndex)
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
      private final NavigableSet<ValueAndSegment> segments = new TreeSet<>();

      public Builder(Interval interval)
      {
        this.interval = interval;
      }

      public Builder addSegment(DataSegment dataSegment)
      {
        if (!interval.contains(dataSegment.getInterval().getStartMillis())) {
          throw new ISE("Failed to add segment to bucket: interval is not covered by this bucket");
        }
        double t0 = convertStart(dataSegment, interval);
        double t1 = convertEnd(dataSegment, interval);

        double leftValue = FastMath.exp(t0) - FastMath.exp(t1);
        double rightValue = FastMath.exp(-t1) - FastMath.exp(-t0);

        ValueAndSegment valueAndSegment = new ValueAndSegment(dataSegment, leftValue, rightValue);

        segments.tailSet(valueAndSegment).forEach(v -> v.leftValue += leftValue);
        segments.headSet(valueAndSegment).forEach(v -> v.rightValue += rightValue);

        ValueAndSegment lower = segments.lower(valueAndSegment);
        if (lower != null) {
          valueAndSegment.leftValue += lower.leftValue;
        }

        ValueAndSegment higher = segments.higher(valueAndSegment);
        if (higher != null) {
          valueAndSegment.rightValue += higher.rightValue;
        }

        segments.add(valueAndSegment);
        return this;
      }

      public Builder removeSegment(DataSegment dataSegment)
      {
        ValueAndSegment valueAndSegment = new ValueAndSegment(dataSegment, 0.0, 0.0);

        if (!segments.remove(valueAndSegment)) {
          return this;
        }

        double t0 = convertStart(dataSegment, interval);
        double t1 = convertEnd(dataSegment, interval);

        double leftValue = FastMath.exp(t0) - FastMath.exp(t1);
        double rightValue = FastMath.exp(-t1) - FastMath.exp(-t0);

        segments.tailSet(valueAndSegment).forEach(v -> v.leftValue -= leftValue);
        segments.headSet(valueAndSegment).forEach(v -> v.rightValue -= rightValue);
        return this;
      }

      public boolean isEmpty()
      {
        return segments.isEmpty();
      }

      public Bucket build()
      {
        ArrayList<DataSegment> segmentsList = new ArrayList<>(segments.size());
        double[] leftSum = new double[segments.size()];
        double[] rightSum = new double[segments.size()];

        int i = 0;
        for (ValueAndSegment valueAndSegment : segments) {
          segmentsList.add(i, valueAndSegment.dataSegment);
          leftSum[i] = valueAndSegment.leftValue;
          rightSum[i] = valueAndSegment.rightValue;
          ++i;
        }
        return new Bucket(
            new Interval(
                interval.getStartMillis(),
                segmentsList.get(segments.size() - 1).getInterval().getEndMillis()
            ),
            segmentsList,
            leftSum,
            rightSum
        );
      }
    }
  }

  static class ValueAndSegment implements Comparable<ValueAndSegment>
  {
    private final DataSegment dataSegment;
    private double leftValue;
    private double rightValue;

    public ValueAndSegment(DataSegment dataSegment, double leftValue, double rightValue)
    {
      this.dataSegment = dataSegment;
      this.leftValue = leftValue;
      this.rightValue = rightValue;
    }

    @Override
    public int compareTo(ValueAndSegment o)
    {
      int c = Comparators.intervalsByStartThenEnd().compare(dataSegment.getInterval(), o.dataSegment.getInterval());
      return (c != 0) ? c : dataSegment.compareTo(o.dataSegment);
    }
  }
}
