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

package io.druid.indexing.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.validation.constraints.NotNull;

public class TaskStorageConfig
{
  public static final String DEFAULT_TASK_OWNER_IR = "default";

  @JsonProperty
  @NotNull
  private Duration recentlyFinishedThreshold = new Period("PT24H").toStandardDuration();

  @JsonProperty
  private String taskOwnerId = DEFAULT_TASK_OWNER_IR;

  @JsonCreator
  public TaskStorageConfig(
      @JsonProperty("recentlyFinishedThreshold") Period period,
      @JsonProperty("taskOwnerId") String taskOwnerId
  )
  {
    if(period != null) {
      this.recentlyFinishedThreshold = period.toStandardDuration();
    }
    if (taskOwnerId != null) {
      this.taskOwnerId = taskOwnerId;
    }
  }

  public Duration getRecentlyFinishedThreshold()
  {
    return recentlyFinishedThreshold;
  }

  public String getTaskOwnerId() {
    return taskOwnerId;
  }
}
