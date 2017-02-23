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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import io.druid.concurrent.Execs;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TasksAndWorkers;
import io.druid.indexing.overlord.WorkerTaskRunner;
import io.druid.indexing.overlord.setup.TwoCloudConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;

import javax.annotation.Nullable;
import java.util.concurrent.ScheduledExecutorService;

import static io.druid.indexing.common.task.TaskLabels.getTaskLabel;

public class TwoCloudWorkerProvisioningStrategy extends AbstractWorkerProvisioningStrategy
{
  private static final Supplier<ScheduledExecutorService> DUMMMY_EXEC_FACTORY = new Supplier<ScheduledExecutorService>()
  {
    @Override
    public ScheduledExecutorService get()
    {
      throw new IllegalStateException("ExecutorService not expected to be created by in-cloud provisioned strategies");
    }
  };

  private final String taskLabel1;
  private final String ipPrefix1;
  private final PendingTaskBasedWorkerProvisioningStrategy provisioningStrategy1;
  private final String taskLabel2;
  private final String ipPrefix2;
  private final PendingTaskBasedWorkerProvisioningStrategy provisioningStrategy2;

  @Inject
  public TwoCloudWorkerProvisioningStrategy(
      Supplier<TwoCloudConfig> twoCloudConfig,
      PendingTaskBasedWorkerProvisioningConfig config,
      ProvisioningSchedulerConfig provisioningSchedulerConfig
  )
  {
    this(
        twoCloudConfig.get().getTaskLabel1(),
        twoCloudConfig.get().getIpPrefix1(),
        twoCloudConfig.get().getWorkerBehaviorConfig1(),
        twoCloudConfig.get().getTaskLabel2(),
        twoCloudConfig.get().getIpPrefix2(),
        twoCloudConfig.get().getWorkerBehaviorConfig2(),
        config,
        provisioningSchedulerConfig
    );
  }

  public TwoCloudWorkerProvisioningStrategy(
      String taskLabel1,
      String ipPrefix1,
      WorkerBehaviorConfig workerBehaviorConfig1,
      String taskLabel2,
      String ipPrefix2,
      WorkerBehaviorConfig workerBehaviorConfig2,
      PendingTaskBasedWorkerProvisioningConfig config,
      ProvisioningSchedulerConfig provisioningSchedulerConfig
  )
  {
    super(
        provisioningSchedulerConfig,
        new Supplier<ScheduledExecutorService>()
        {
          @Override
          public ScheduledExecutorService get()
          {
            return Execs.scheduledSingleThreaded("TwoCloudWorkerProvisioningStrategy-provisioner-%d");
          }
        }
    );
    this.taskLabel1 = taskLabel1;
    this.taskLabel2 = taskLabel2;
    this.ipPrefix1 = ipPrefix1;
    this.ipPrefix2 = ipPrefix2;
    provisioningStrategy1 = new PendingTaskBasedWorkerProvisioningStrategy(
        config,
        Suppliers.ofInstance(workerBehaviorConfig1),
        provisioningSchedulerConfig,
        DUMMMY_EXEC_FACTORY,
        ipPrefix1
    );
    provisioningStrategy2 = new PendingTaskBasedWorkerProvisioningStrategy(
        config,
        Suppliers.ofInstance(workerBehaviorConfig2),
        provisioningSchedulerConfig,
        DUMMMY_EXEC_FACTORY,
        ipPrefix2
    );
  }

  @Override
  Provisioner makeProvisioner(TasksAndWorkers runner)
  {
    final TaskPredicate taskPredicate1 = new TaskPredicate(taskLabel1, true);
    final TaskPredicate taskPredicate2 = new TaskPredicate(taskLabel2, false);

    final Provisioner provisioner1 = provisioningStrategy1.makeProvisioner(
        new TasksAndWorkersFilteredByIp((WorkerTaskRunner) runner, ipPrefix1, taskPredicate1)
    );
    final Provisioner provisioner2 = provisioningStrategy2.makeProvisioner(
        new TasksAndWorkersFilteredByIp((WorkerTaskRunner) runner, ipPrefix2, taskPredicate2)
    );
    return new Provisioner()
    {
      @Override
      public boolean doTerminate()
      {
        // Always try to terminate in both clouds before returning from this method
        boolean terminated1 = provisioner1.doTerminate();
        boolean terminated2 = provisioner2.doTerminate();
        return terminated1 || terminated2;
      }

      @Override
      public boolean doProvision()
      {
        // Always try to provision in both clouds before returning from this method
        boolean provisioned1 = provisioner1.doProvision();
        boolean provisioned2 = provisioner2.doProvision();
        return provisioned1 || provisioned2;
      }

      @Override
      public ScalingStats getStats()
      {
        ScalingStats stats1 = provisioner1.getStats();
        ScalingStats stats2 = provisioner2.getStats();
        ScalingStats stats = new ScalingStats(stats1.size() + stats2.size());
        stats.addAll(stats1);
        stats.addAll(stats2);
        return stats;
      }
    };
  }

  private class TaskPredicate implements Predicate<Task> {
    private final String taskLabel;
    private final boolean acceptNullLabel;

    public TaskPredicate(String taskLabel, boolean acceptNullLabel) {
      this.taskLabel = taskLabel;
      this.acceptNullLabel = acceptNullLabel;
    }

    @Override
    public boolean apply(@Nullable Task task)
    {
      if (task == null) {
        return false;
      }
      String label = getTaskLabel(task);
      return label == null ? acceptNullLabel : label.equals(taskLabel);
    }
  }

}
