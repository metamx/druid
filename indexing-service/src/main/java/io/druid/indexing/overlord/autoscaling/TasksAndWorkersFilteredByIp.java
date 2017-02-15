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

package io.druid.indexing.overlord.autoscaling;

import com.google.common.base.Predicate;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.ImmutableWorkerInfo;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.TasksAndWorkers;
import io.druid.indexing.overlord.WorkerTaskRunner;
import io.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import io.druid.indexing.worker.Worker;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;

public class TasksAndWorkersFilteredByIp implements TasksAndWorkers
{
  private final WorkerTaskRunner delegate;
  private final String ipPrefix;

  public TasksAndWorkersFilteredByIp(WorkerTaskRunner delegate, String ipPrefix)
  {
    this.delegate = delegate;
    this.ipPrefix = ipPrefix;
  }

  private Collection<Worker> filter(Collection<Worker> workers)
  {
    Collection<Worker> filtered = new ArrayList<>(workers.size());
    for (Worker worker : workers) {
      if (worker.getIp().startsWith(ipPrefix)) {
        filtered.add(worker);
      }
    }
    return filtered;
  }

  @Override
  public Collection<ImmutableWorkerInfo> getWorkers()
  {
    Collection<ImmutableWorkerInfo> workers = delegate.getWorkers();
    Collection<ImmutableWorkerInfo> filtered = new ArrayList<>(workers.size());
    for (ImmutableWorkerInfo workerInfo : workers) {
      if (workerInfo.getWorker().getIp().startsWith(ipPrefix)) {
        filtered.add(workerInfo);
      }
    }
    return filtered;
  }

  @Override
  public Collection<Worker> getLazyWorkers()
  {
    return filter(delegate.getLazyWorkers());
  }

  @Override
  public Collection<Worker> markWorkersLazy(final Predicate<ImmutableWorkerInfo> isLazyWorker, int maxWorkers)
  {
    Predicate<ImmutableWorkerInfo> isLazyWorkerWithProperIp = new Predicate<ImmutableWorkerInfo>()
    {
      @Override
      public boolean apply(@Nullable ImmutableWorkerInfo input)
      {
        return isLazyWorker.apply(input) && input.getWorker().getIp().startsWith(ipPrefix);
      }
    };
    return filter(delegate.markWorkersLazy(isLazyWorkerWithProperIp, maxWorkers));
  }

  @Override
  public WorkerTaskRunnerConfig getConfig()
  {
    return delegate.getConfig();
  }

  @Override
  public Collection<Task> getPendingTaskPayloads()
  {
    // TODO https://metamarkets.atlassian.net/browse/BACKEND-658
    return delegate.getPendingTaskPayloads();
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
  {
    // TODO https://metamarkets.atlassian.net/browse/BACKEND-658
    return delegate.getPendingTasks();
  }
}
