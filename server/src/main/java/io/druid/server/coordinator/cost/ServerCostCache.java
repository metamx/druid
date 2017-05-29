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
import io.druid.timeline.DataSegment;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ServerCostCache
{
  private final SegmentCostCache allSegmentsCostCache;
  private final Map<String, SegmentCostCache> segmentsPerDataSource;

  public ServerCostCache(
      SegmentCostCache allSegmentsCostCache,
      Map<String, SegmentCostCache> segmentsCostPerDataSource
  )
  {
    this.allSegmentsCostCache = Preconditions.checkNotNull(allSegmentsCostCache);
    this.segmentsPerDataSource = Preconditions.checkNotNull(segmentsCostPerDataSource);
  }

  public double computeCost(DataSegment segment)
  {
    return allSegmentsCostCache.cost(segment) + computeDataSourceCost(segment);
  }

  private double computeDataSourceCost(DataSegment segment)
  {
    SegmentCostCache costCache = segmentsPerDataSource.get(segment.getDataSource());
    return (costCache == null) ? 0.0 : costCache.cost(segment);
  }

  public static Builder builder()
  {
    return new Builder();
  }


  public static class Builder
  {
    private final SegmentCostCache.Builder allSegmentsCostCache = SegmentCostCache.builder();
    private final Map<String, SegmentCostCache.Builder> segmentsPerDataSource = new HashMap<>();

    public void addSegment(DataSegment dataSegment)
    {
      allSegmentsCostCache.addSegment(dataSegment);
      segmentsPerDataSource.computeIfAbsent(
          dataSegment.getDataSource(),
          d -> SegmentCostCache.builder()
      ).addSegment(dataSegment);
    }

    public void removeSegment(DataSegment dataSegment)
    {
      allSegmentsCostCache.removeSegement(dataSegment);
      SegmentCostCache.Builder builder = segmentsPerDataSource.get(dataSegment.getDataSource());
      builder.removeSegement(dataSegment);
      if (builder.size() == 0) {
        segmentsPerDataSource.remove(dataSegment.getDataSource());
      }
    }

    public ServerCostCache build()
    {
      return new ServerCostCache(
          allSegmentsCostCache.build(),
          segmentsPerDataSource
              .entrySet()
              .stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build()))
      );
    }
  }
}
