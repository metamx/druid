package io.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import io.druid.indexing.overlord.setup.TwoCloudConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.indexing.overlord.setup.WorkerSelectStrategy;

import java.util.Map;

import static io.druid.indexing.common.task.TaskLabels.getTaskLabel;

public class TwoCloudWorkerSelectStrategy implements WorkerSelectStrategy
{
  private final TwoCloudConfig twoCloudConfig;

  @JsonCreator
  public TwoCloudWorkerSelectStrategy(
      @JsonProperty("twoCloudConfig") TwoCloudConfig twoCloudConfig
  )
  {
    this.twoCloudConfig = twoCloudConfig;
  }

  @Override
  public Optional<ImmutableWorkerInfo> findWorkerForTask(
      WorkerTaskRunnerConfig config, ImmutableMap<String, ImmutableWorkerInfo> zkWorkers, Task task
  )
  {
    String taskLabel = getTaskLabel(task);
    String ipFilter = twoCloudConfig.getIpFilter(taskLabel);
    WorkerBehaviorConfig workerBehaviorConfig = twoCloudConfig.getWorkerBehaviorConfig(taskLabel);
    WorkerSelectStrategy workerSelectStrategy = workerBehaviorConfig.getSelectStrategy();
    return workerSelectStrategy.findWorkerForTask(config, filterWorkers(ipFilter, zkWorkers), task);
  }

  private ImmutableMap<String, ImmutableWorkerInfo> filterWorkers(String ipFilter, ImmutableMap<String, ImmutableWorkerInfo> zkWorkers) {
    ImmutableMap.Builder<String, ImmutableWorkerInfo> filtered = ImmutableMap.builder();
    for (Map.Entry<String, ImmutableWorkerInfo> e : zkWorkers.entrySet()) {
      if (e.getValue().getWorker().getIp().startsWith(ipFilter)) {
        filtered.put(e);
      }
    }
    return filtered.build();
  }
}
