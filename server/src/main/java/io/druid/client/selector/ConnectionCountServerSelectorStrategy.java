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

package io.druid.client.selector;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.guava.Comparators;
import io.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

public class ConnectionCountServerSelectorStrategy implements ServerSelectorStrategy
{
  private static final Comparator<QueryableDruidServer> comparator = new Comparator<QueryableDruidServer>()
  {
    @Override
    public int compare(QueryableDruidServer left, QueryableDruidServer right)
    {
      return Ints.compare(left.getClient().getNumOpenConnections(), right.getClient().getNumOpenConnections());
    }
  };

  @Override
  public QueryableDruidServer pick(Set<QueryableDruidServer> servers, DataSegment segment)
  {
    return Collections.min(servers, comparator);
  }

  @Override
  public List<QueryableDruidServer> pick(
      Set<QueryableDruidServer> servers, DataSegment segment, int numServersToPick
  )
  {
    if (servers.size() <= numServersToPick) {
      return Lists.newArrayList(servers);
    }
    PriorityQueue<QueryableDruidServer> maxHeap = new PriorityQueue<>(numServersToPick, Comparators.inverse(comparator));
    for (QueryableDruidServer server : servers) {
      if (maxHeap.size() < numServersToPick) {
        maxHeap.add(server);
      } else {
        if (comparator.compare(server, maxHeap.peek()) < 0) {
          maxHeap.poll();
          maxHeap.add(server);
        }
      }
    }
    return Lists.newArrayList(maxHeap);
  }
}
