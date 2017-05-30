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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.druid.client.ServerInventoryView;
import io.druid.client.ServerView;
import io.druid.concurrent.Execs;
import io.druid.guice.ManageLifecycle;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordinator.cost.ClusterCostCache;
import io.druid.timeline.DataSegment;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@ManageLifecycle
public class CachingCostBalancerStrategyFactory implements BalancerStrategyFactory
{
  private final ServerInventoryView serverInventoryView;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicReference<ExecutorService> executorRef = new AtomicReference<>(null);
  private volatile boolean initialized = false;
  private ClusterCostCache.Builder clusterCostCacheBuilder = ClusterCostCache.builder();

  @Inject
  public CachingCostBalancerStrategyFactory(ServerInventoryView serverInventoryView)
  {
    this.serverInventoryView = Preconditions.checkNotNull(serverInventoryView);
  }

  @LifecycleStart
  public void start()
  {
    if (!executorRef.compareAndSet(null, Execs.singleThreaded("CachingCostBalancerStrategy-executor"))) {
      throw new ISE("CachingCostBalancerStrategyFactory is already started or has been stopped");
    }
    if (!started.compareAndSet(false, true)) {
      throw new ISE("CachingCostBalancerStrategyFactory is already started");
    }
    serverInventoryView.registerSegmentCallback(
        executorRef.get(),
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(
              DruidServerMetadata server, DataSegment segment
          )
          {
            clusterCostCacheBuilder.addSegment(server.getName(), segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(
              DruidServerMetadata server, DataSegment segment
          )
          {
            clusterCostCacheBuilder.removeSegment(server.getName(), segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentViewInitialized()
          {
            initialized = true;
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );

    serverInventoryView.registerServerCallback(
        executorRef.get(),
        server -> {
          clusterCostCacheBuilder.removeServer(server.getName());
          return ServerView.CallbackAction.CONTINUE;
        }
    );
  }

  @LifecycleStop
  public void stop()
  {
    if (!started.compareAndSet(true, false)) {
      throw new ISE("CachingCostBalancerStrategyFactory is not started");
    }
    executorRef.get().shutdownNow();
  }

  @Override
  public BalancerStrategy createBalancerStrategy(ListeningExecutorService exec)
  {
    return initialized
           ? new CachingCostBalancerStrategy(clusterCostCacheBuilder.build(), exec)
           : new CostBalancerStrategy(exec);
  }
}
