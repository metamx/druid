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

package io.druid.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.InputStreamResponseHandler;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.IOUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Interval;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

public class CostBalancerBenchmarkUtil
{
  public static String getSegmentsJson(URL url)
  {
    Lifecycle lifecycle = new Lifecycle();
    HttpClient httpClient = HttpClientInit.createClient(
        HttpClientConfig.builder().build(),
        lifecycle
    );
    Future<InputStream> future = httpClient.go(
        new Request(
            HttpMethod.GET,
            url
        ),
        new InputStreamResponseHandler()
    );

    try {
      InputStream in = future.get();
      return IOUtils.toString(in);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String getSegmentsJson(String coordinatorHostPort, String serverHostPort)
  {
    try {
      return getSegmentsJson(new URL(String.format(
          "http://%s/druid/coordinator/v1/servers/%s",
          coordinatorHostPort,
          serverHostPort
      )));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static List<DataSegment> parseDataSegments(String json)
  {
    try {
      List<DataSegment> dataSegments = new ArrayList<>();
      ObjectMapper objectMapper = new DefaultObjectMapper();
      Iterator<JsonNode> nodes = objectMapper.readTree(json).get("segments").elements();
      while (nodes.hasNext()) {
        JsonNode node = nodes.next();
        DataSegment segment = new DataSegment(
            node.get("dataSource").asText(),
            objectMapper.treeToValue(node.get("interval"), Interval.class),
            node.get("version").asText(),
            ImmutableMap.<String, Object>of(),
            ImmutableList.<String>of(),
            ImmutableList.<String>of(),
            null,
            node.get("binaryVersion").asInt(),
            node.get("size").asLong()
        );
        dataSegments.add(segment);
      }
      return dataSegments;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws Exception
  {
    String json = getSegmentsJson("172.19.72.53:23709", "172.19.68.129:25580");
    List<DataSegment> dataSegments = parseDataSegments(json);
    System.out.println(dataSegments.size());
  }
}
