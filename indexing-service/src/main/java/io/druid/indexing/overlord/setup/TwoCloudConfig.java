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

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TwoCloudConfig
{
  private final String ipPrefix1;
  private final WorkerBehaviorConfig workerBehaviorConfig1;
  private final String ipPrefix2;
  private final WorkerBehaviorConfig workerBehaviorConfig2;

  @JsonCreator
  public TwoCloudConfig(
      @JsonProperty("ipPrefix1") String ipPrefix1,
      @JsonProperty("workerBehaviorConfig1") WorkerBehaviorConfig workerBehaviorConfig1,
      @JsonProperty("ipPrefix2") String ipPrefix2,
      @JsonProperty("workerBehaviorConfig2") WorkerBehaviorConfig workerBehaviorConfig2
  )
  {
    this.ipPrefix1 = ipPrefix1;
    this.workerBehaviorConfig1 = workerBehaviorConfig1;
    this.ipPrefix2 = ipPrefix2;
    this.workerBehaviorConfig2 = workerBehaviorConfig2;
  }

  @JsonProperty
  public String getIpPrefix1()
  {
    return ipPrefix1;
  }

  @JsonProperty
  public WorkerBehaviorConfig getWorkerBehaviorConfig1()
  {
    return workerBehaviorConfig1;
  }

  @JsonProperty
  public String getIpPrefix2()
  {
    return ipPrefix2;
  }

  @JsonProperty
  public WorkerBehaviorConfig getWorkerBehaviorConfig2()
  {
    return workerBehaviorConfig2;
  }

  @Override
  public String toString()
  {
    return "TwoCloudConfig{" +
           "ipPrefix1='" + ipPrefix1 + '\'' +
           ", workerBehaviorConfig1=" + workerBehaviorConfig1 +
           ", ipPrefix2='" + ipPrefix2 + '\'' +
           ", workerBehaviorConfig2=" + workerBehaviorConfig2 +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TwoCloudConfig that = (TwoCloudConfig) o;

    if (ipPrefix1 != null ? !ipPrefix1.equals(that.ipPrefix1) : that.ipPrefix1 != null) {
      return false;
    }
    if (workerBehaviorConfig1 != null
        ? !workerBehaviorConfig1.equals(that.workerBehaviorConfig1)
        : that.workerBehaviorConfig1 != null) {
      return false;
    }
    if (ipPrefix2 != null ? !ipPrefix2.equals(that.ipPrefix2) : that.ipPrefix2 != null) {
      return false;
    }
    return workerBehaviorConfig2 != null
           ? workerBehaviorConfig2.equals(that.workerBehaviorConfig2)
           : that.workerBehaviorConfig2 == null;
  }

  @Override
  public int hashCode()
  {
    int result = ipPrefix1 != null ? ipPrefix1.hashCode() : 0;
    result = 31 * result + (workerBehaviorConfig1 != null ? workerBehaviorConfig1.hashCode() : 0);
    result = 31 * result + (ipPrefix2 != null ? ipPrefix2.hashCode() : 0);
    result = 31 * result + (workerBehaviorConfig2 != null ? workerBehaviorConfig2.hashCode() : 0);
    return result;
  }
}
