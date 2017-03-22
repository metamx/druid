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

package io.druid.indexing.overlord;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.task.Task;
import org.joda.time.Interval;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Delegates all functionality to LocalTaskLockbox,
 * makes additional check that lock is not acquired by RemoteTaskLockbox
 */
public class DistributedTaskLockbox implements TaskLockbox
{
  private static final int TASK_LOCKS_CACHE_SIZE = 1000;
  private static final long RETRY_AFTER_MILLIS = TimeUnit.SECONDS.toMillis(5);

  private final TaskStorage taskStorage;
  private final TaskLockbox delegate;
  private final LinkedHashSet<TaskLock> taskLocks;

  @Inject
  public DistributedTaskLockbox(
      TaskStorage taskStorage
  )
  {
    this.taskStorage = taskStorage;
    this.delegate = new LocalTaskLockbox(taskStorage);
    this.taskLocks = new LinkedHashSet<>(TASK_LOCKS_CACHE_SIZE);
  }

  @Override
  public void syncFromStorage()
  {
    delegate.syncFromStorage();
  }

  @Override
  public TaskLock lock(
      final Task task, final Interval interval
  ) throws InterruptedException
  {
    Optional<TaskLock> taskLock = Optional.absent();
    while (!taskLock.isPresent()) {
      taskLock = tryLock(task, interval);
      if (!taskLock.isPresent()) {
        Thread.sleep(RETRY_AFTER_MILLIS);
      }
    }
    return taskLock.get();
  }

  @Override
  public Optional<TaskLock> tryLock(
      final Task task, final Interval interval
  )
  {
    return tryLock(task, interval, Optional.<String>absent());
  }

  @Override
  public Optional<TaskLock> tryLock(
      final Task task, final Interval interval, final Optional<String> preferredVersion
  )
  {
    Optional<TaskLock> taskLock = delegate.tryLock(task, interval, preferredVersion);
    if (taskLock.isPresent()) {
      // checking remote locks is redundant if task has been added
      // to the existing lock (with same group id)
      if (!isExistedTaskLock(taskLock.get())) {
        if (isLockedRemotely(task, interval)) {
          delegate.unlock(task, interval);
          return Optional.absent();
        }
        addTaskLock(taskLock.get());
      }
    }
    return taskLock;
  }

  @Override
  public void unlock(final Task task, final Interval interval)
  {
    delegate.unlock(task, interval);
  }

  @Override
  public List<TaskLock> findLocksForTask(final Task task)
  {
    return delegate.findLocksForTask(task);
  }

  @Override
  public void add(final Task task)
  {
    delegate.add(task);
  }

  @Override
  public void remove(final Task task)
  {
    delegate.remove(task);
  }

  private boolean isLockedRemotely(final Task task, final Interval interval)
  {
    List<TaskLock> remoteTaskLocks = taskStorage.getRemoteActiveLocks();
    for (TaskLock remoteTaskLock : remoteTaskLocks) {
      if (remoteTaskLock.getDataSource().equals(task.getDataSource()) &&
          remoteTaskLock.getInterval().overlaps(interval) &&
          !remoteTaskLock.getGroupId().equals(task.getGroupId())) {
        return true;
      }
    }
    return false;
  }

  private boolean isExistedTaskLock(TaskLock taskLock)
  {
    synchronized (taskLocks) {
      return taskLocks.contains(taskLock);
    }
  }

  private void addTaskLock(TaskLock taskLock)
  {
    synchronized (taskLocks) {
      Iterator<TaskLock> it = taskLocks.iterator();
      while (taskLocks.size() > TASK_LOCKS_CACHE_SIZE) {
        it.next();
        it.remove();
      }
      taskLocks.add(taskLock);
    }
  }
}
