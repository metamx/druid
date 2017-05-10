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

import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 */
public class ServerSelector implements DiscoverySelector<QueryableDruidServer>
{

  private final TreeMap<Integer, Set<QueryableDruidServer>> servers;

  private final TierSelectorStrategy strategy;

  private final AtomicReference<DataSegment> segment;

  public ServerSelector(
      DataSegment segment,
      TierSelectorStrategy strategy
  )
  {
    this.segment = new AtomicReference<>(segment);
    this.strategy = strategy;
    this.servers = new TreeMap<>(strategy.getComparator());
  }

  public DataSegment getSegment()
  {
    return segment.get();
  }

  public void addServerAndUpdateSegment(
      QueryableDruidServer server, DataSegment segment
  )
  {
    synchronized (this) {
      this.segment.set(segment);
      int priority = server.getServer().getPriority();
      Set<QueryableDruidServer> priorityServers = servers.get(priority);
      if (priorityServers == null) {
        priorityServers = new HashSet<>();
        servers.put(priority, priorityServers);
      }
      priorityServers.add(server);
    }
  }

  public boolean removeServer(QueryableDruidServer server)
  {
    synchronized (this) {
      int priority = server.getServer().getPriority();
      Set<QueryableDruidServer> priorityServers = servers.get(priority);
      if (priorityServers == null) {
        return false;
      }
      return priorityServers.remove(server);
    }
  }

  public boolean isEmpty()
  {
    synchronized (this) {
      return servers.isEmpty();
    }
  }

  public List<DruidServerMetadata> getCandidates(final int numCandidates) {
    synchronized (this) {
      return strategy.pick(servers, segment.get(), numCandidates)
          .stream()
          .map(server -> server.getServer().getMetadata())
          .collect(Collectors.toList());
    }
  }

  @Override
  public QueryableDruidServer pick()
  {
    synchronized (this) {
      return strategy.pick(servers, segment.get());
    }
  }
}
