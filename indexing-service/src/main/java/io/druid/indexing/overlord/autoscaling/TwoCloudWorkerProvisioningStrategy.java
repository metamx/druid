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
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.concurrent.Execs;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.task.TaskLabels;
import io.druid.indexing.overlord.TasksAndWorkers;
import io.druid.indexing.overlord.WorkerTaskRunner;
import io.druid.indexing.overlord.setup.BaseWorkerBehaviorConfig;
import io.druid.indexing.overlord.setup.TwoCloudConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;

import javax.annotation.Nullable;
import java.util.concurrent.ScheduledExecutorService;

public class TwoCloudWorkerProvisioningStrategy extends AbstractWorkerProvisioningStrategy
{
  private static final Logger log = new Logger(TwoCloudWorkerProvisioningStrategy.class);

  private static final Supplier<ScheduledExecutorService> DUMMY_EXEC_FACTORY = new Supplier<ScheduledExecutorService>()
  {
    @Override
    public ScheduledExecutorService get()
    {
      throw new IllegalStateException("ExecutorService not expected to be created by in-cloud provisioned strategies");
    }
  };

  private final PendingTaskBasedWorkerProvisioningConfig pendingProvisioningConfig;
  private final Supplier<BaseWorkerBehaviorConfig> workerBehaviorConfigSupplier;

  @Inject
  public TwoCloudWorkerProvisioningStrategy(
      final Supplier<BaseWorkerBehaviorConfig> workerBehaviorConfigSupplier,
      PendingTaskBasedWorkerProvisioningConfig pendingProvisioningConfig,
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
    this.pendingProvisioningConfig = pendingProvisioningConfig;
    this.workerBehaviorConfigSupplier = workerBehaviorConfigSupplier;
  }

  @Override
  Provisioner makeProvisioner(final TasksAndWorkers runner)
  {
    return new Provisioner()
    {
      private final ScalingStats scalingStats = new ScalingStats(pendingProvisioningConfig.getNumEventsToTrack() * 2);
      private BaseWorkerBehaviorConfig lastWorkerBehaviorConfig;
      private Provisioner provisioner1;
      private Provisioner provisioner2;

      private void updateDelegateProvisioners()
      {
        final BaseWorkerBehaviorConfig newConfig = workerBehaviorConfigSupplier.get();
        if (newConfig != lastWorkerBehaviorConfig) {
          log.info("New workerBehaviourConfig: [%s]", newConfig);
          if (newConfig instanceof TwoCloudConfig) {
            updateTwoCloudProvisioners((TwoCloudConfig) newConfig);
          } else if (newConfig instanceof WorkerBehaviorConfig) {
            updateOneCloudProvisioner((WorkerBehaviorConfig) newConfig);
          } else {
            throw new ISE("Unknown type of BaseWorkerBehaviorConfig: [%s]", newConfig);
          }
          lastWorkerBehaviorConfig = newConfig;
        }
      }

      private void updateOneCloudProvisioner(WorkerBehaviorConfig newWorkerBehaviorConfig)
      {
        provisioner1 = makeDelegateProvisioner(
            newWorkerBehaviorConfig,
            PendingTaskBasedWorkerProvisioningStrategy.DEFAULT_DUMMY_WORKER_IP,
            null,
            false
        );
        provisioner2 = null;
      }

      private void updateTwoCloudProvisioners(TwoCloudConfig newConfig)
      {
        provisioner1 = makeDelegateProvisioner(
            newConfig.getCloud1Config(),
            newConfig.getIpPrefix1(),
            newConfig.getTaskLabel1(),
            true
        );
        provisioner1 = makeDelegateProvisioner(
            newConfig.getCloud2Config(),
            newConfig.getIpPrefix2(),
            newConfig.getTaskLabel2(),
            false
        );
      }

      private Provisioner makeDelegateProvisioner(
          final WorkerBehaviorConfig workerBehaviorConfig,
          String ipPrefix,
          @Nullable String taskLabel,
          boolean acceptNullLabel
      )
      {
        PendingTaskBasedWorkerProvisioningStrategy delegateProvisioningStrategy = new
            PendingTaskBasedWorkerProvisioningStrategy(
            pendingProvisioningConfig,
            new Supplier<BaseWorkerBehaviorConfig>()
            {
              @Override
              public WorkerBehaviorConfig get()
              {
                return workerBehaviorConfig;
              }
            },
            getProvisioningSchedulerConfig(),
            DUMMY_EXEC_FACTORY,
            ipPrefix
        );
        TasksAndWorkers tasksAndWorkers;
        if (taskLabel != null) {
          TaskPredicate taskPredicate = new TaskPredicate(taskLabel, acceptNullLabel);
          tasksAndWorkers = new TasksAndWorkersFilteredByIp((WorkerTaskRunner) runner, ipPrefix, taskPredicate);
        } else {
          tasksAndWorkers = runner;
        }
        return delegateProvisioningStrategy.makeProvisioner(tasksAndWorkers, scalingStats);
      }

      @Override
      public boolean doTerminate()
      {
        updateDelegateProvisioners();
        log.info("Try terminate in the 1st cloud");
        boolean terminated1 = provisioner1.doTerminate();
        log.info("Terminated in the 1st cloud: %s", terminated1);
        boolean terminated2;
        if (provisioner2 != null) {
          log.info("Try terminate in the 2nd cloud");
          terminated2 = provisioner2 != null && provisioner2.doTerminate();
          log.info("Terminated in the 2nd cloud: %s", terminated2);
        } else {
          log.info("No config for the 2nd cloud");
          terminated2 = false;
        }
        return terminated1 || terminated2;
      }

      @Override
      public boolean doProvision()
      {
        updateDelegateProvisioners();
        log.info("Try provision in the 1st cloud");
        boolean provisioned1 = provisioner1.doProvision();
        log.info("Provisioned in the 1st cloud: %s", provisioned1);
        boolean provisioned2;
        if (provisioner2 != null) {
          log.info("Try provision in the 2nd cloud");
          provisioned2 = provisioner2.doProvision();
          log.info("Provisioned in the 2nd cloud: %s", provisioned2);
        } else {
          log.info("No config for the 2nd cloud");
          provisioned2 = false;
        }
        return provisioned1 || provisioned2;
      }

      @Override
      public ScalingStats getStats()
      {
        return scalingStats;
      }
    };
  }

  private class TaskPredicate implements Predicate<Task> {
    private final String taskLabel;
    private final boolean acceptNullLabel;

    TaskPredicate(String taskLabel, boolean acceptNullLabel) {
      this.taskLabel = taskLabel;
      this.acceptNullLabel = acceptNullLabel;
    }

    @Override
    public boolean apply(@Nullable Task task)
    {
      if (task == null) {
        return false;
      }
      String label = TaskLabels.getTaskLabel(task);
      return label == null ? acceptNullLabel : label.equals(taskLabel);
    }
  }

}
