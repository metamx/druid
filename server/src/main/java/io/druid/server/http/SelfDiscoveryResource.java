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

package io.druid.server.http;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscovery;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.guice.annotations.Self;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.server.DruidNode;
import io.druid.server.http.security.StateResourceFilter;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

/**
 * This class is annotated {@link Singleton} rather than {@link io.druid.guice.LazySingleton}, because it adds
 * a lifecycle handler in the constructor, that should happen before the lifecycle is started, i. e. eagerly during the
 * DI configuration phase.
 */
@Singleton
@Path("/selfDiscovered")
@ResourceFilters(StateResourceFilter.class)
public class SelfDiscoveryResource
{
  private boolean selfDiscovered = false;

  @Inject
  public SelfDiscoveryResource(
      @Self DruidNode thisDruidNode,
      @Self String thisNodeType,
      DruidNodeDiscoveryProvider nodeDiscoveryProvider,
      Lifecycle lifecycle
  )
  {
    Lifecycle.Handler selfDiscoveryListenerRegistrator = new Lifecycle.Handler()
    {
      @Override
      public void start()
      {
        registerSelfDiscoveryListener(thisDruidNode, thisNodeType, nodeDiscoveryProvider);
      }

      @Override
      public void stop()
      {
        // do nothing
      }
    };
    // Using Lifecycle.Stage.LAST because DruidNodeDiscoveryProvider should be already started when
    // registerSelfDiscoveryListener() is called.
    lifecycle.addHandler(selfDiscoveryListenerRegistrator, Lifecycle.Stage.LAST);
  }

  private void registerSelfDiscoveryListener(
      DruidNode thisDruidNode,
      String thisNodeType,
      DruidNodeDiscoveryProvider nodeDiscoveryProvider
  )
  {
    nodeDiscoveryProvider.getForNodeType(thisNodeType).registerListener(new DruidNodeDiscovery.Listener()
    {
      @Override
      public void nodesAdded(List<DiscoveryDruidNode> nodes)
      {
        if (selfDiscovered) {
          return;
        }
        for (DiscoveryDruidNode node : nodes) {
          if (node.getDruidNode().equals(thisDruidNode)) {
            selfDiscovered = true;
            break;
          }
        }
      }

      @Override
      public void nodesRemoved(List<DiscoveryDruidNode> nodes)
      {
        // do nothing
      }
    });
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSelfDiscovered()
  {
    return Response.ok(Collections.singletonMap("selfDiscovered", selfDiscovered)).build();
  }
}
