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

package io.druid.grpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.druid.client.DirectDruidClient;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.Yielders;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryPlus;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.Result;
import io.druid.server.QueryManager;
import io.grpc.BindableService;
import io.grpc.KnownLength;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.Status;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class QueryServer
{
  private static final Logger LOG = new Logger(QueryServer.class);
  private static final String FULL_SERVICE_NAME = "druid";
  private static final String FULL_METHOD_NAME = MethodDescriptor.generateFullMethodName(FULL_SERVICE_NAME, "query");
  private final ObjectMapper mapper;
  private final QueryManager queryManager;
  private final QuerySegmentWalker texasRanger;
  private final AtomicReference<Server> server = new AtomicReference<>(null);
  @VisibleForTesting
  final QueryImpl queryImpl = new QueryImpl();

  @Inject
  public QueryServer(
      @Json ObjectMapper mapper,
      QueryManager queryManager,
      QuerySegmentWalker texasRanger
  )
  {
    this.mapper = mapper;
    this.queryManager = queryManager;
    this.texasRanger = texasRanger;
  }

  @LifecycleStart
  public void start()
  {
    final Server server = ServerBuilder
        .forPort(9987)
        .addService(queryImpl)
        .build();
    if (!this.server.compareAndSet(null, server)) {
      // If there really is some weird race here, the `this.server.get()` is also racy
      throw new ISE("Expected no server set, instead found %s", this.server.get());
    }
    try {
      server.start();
    }
    catch (IOException e) {
      this.server.set(null);
      throw new RE(e, "Failed to start gRPC server on port %d", 9987);
    }
  }

  @LifecycleStop
  public synchronized void stop()
  {
    final Server server = this.server.get();
    if (server != null) {
      server.shutdown();
      if (!this.server.compareAndSet(server, null)) {
        LOG.warn("Strange race condition, not removing old server setting");
      }
      try {
        if (!server.awaitTermination(90000, TimeUnit.MILLISECONDS)) {
          throw new TimeoutException(StringUtils.safeFormat(
              "Timed out waiting for termination. Waited %d ms",
              90000
          ));
        }
      }
      catch (InterruptedException | TimeoutException e) {
        LOG.warn(e, "Problem during shutdown of gRPC server, potentially unclean shutdown.");
      }
    }
  }

  @VisibleForTesting
  class QueryImpl implements BindableService
  {
    @VisibleForTesting
    final MethodDescriptor.Marshaller<Query> QUERY_MARSHALL = new MethodDescriptor.Marshaller<Query>()
    {
      @Override
      public InputStream stream(Query value)
      {
        try {
          return new ByteArrayInputStream(mapper.writeValueAsBytes(value));
        }
        catch (IOException ioe) {
          throw new RE(ioe, "Error writing query value %s", value);
        }
      }

      @Override
      public Query parse(InputStream stream)
      {
        try {
          return mapper.readValue(stream, Query.class);
        }
        catch (IOException e) {
          throw new RE(e, "Error parsing gRPC query request");
        }
      }
    };
    @VisibleForTesting
    final MethodDescriptor.Marshaller<Result> RESULT_MARSHALL = new MethodDescriptor.Marshaller<Result>()
    {
      @Override
      public InputStream stream(Result value)
      {
        try {
          return new ByteArrayInputStream(mapper.writeValueAsBytes(value));
        }
        catch (IOException ioe) {
          throw new RE(ioe, "Error writing return value %s", value);
        }
      }

      @Override
      public Result parse(InputStream stream)
      {
        try {
          return mapper.readValue(stream, Result.class);
        }
        catch (IOException ioe) {
          throw new RE(ioe, "Error parsing query result from stream");
        }
      }
    };
    @VisibleForTesting
    final MethodDescriptor<Query, Result> methodDescriptor = MethodDescriptor
        .<Query, Result>newBuilder()
        .setFullMethodName(FULL_METHOD_NAME)
        .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
        .setRequestMarshaller(QUERY_MARSHALL)
        .setResponseMarshaller(RESULT_MARSHALL)
        // If segment version changes, might not be the same result
        .setIdempotent(false)
        // Limited to processing threads
        .setSafe(false)
        .build();
    @VisibleForTesting
    final ServiceDescriptor SERVICE_DESCRIPTOR = ServiceDescriptor
        .newBuilder(FULL_SERVICE_NAME)
        .addMethod(methodDescriptor)
        .build();
    @VisibleForTesting
    final Metadata.Key<String> QUERY_ID_KEY = Metadata.Key.of(
        "X-Druid-Query-Id",
        new Metadata.AsciiMarshaller<String>()
        {
          @Override
          public String toAsciiString(String value)
          {
            return value;
          }

          @Override
          public String parseAsciiString(String serialized)
          {
            return serialized;
          }
        }
    );
    @VisibleForTesting
    final ServerCallHandler<Query, Result> SERVICE_CALL_HANDLER = new ServerCallHandler<Query, Result>()
    {
      @Override
      public ServerCall.Listener<Query> startCall(
          final ServerCall<Query, Result> call,
          final Metadata headers
      )
      {
        final AtomicBoolean queryReceived = new AtomicBoolean(false);
        final String id;
        if (headers.containsKey(QUERY_ID_KEY)) {
          id = headers.get(QUERY_ID_KEY);
        } else {
          id = UUID.randomUUID().toString();
        }
        call.request(1);
        return new ServerCall.Listener<Query>()
        {
          public void onMessage(Query query)
          {
            if (!queryReceived.compareAndSet(false, true)) {
              throw new ISE("Query [%s] already called once", id);
            }
            final Map<String, Object> responseContext = DirectDruidClient.makeResponseContextForQuery(
                query,
                System.currentTimeMillis()
            );
            final Sequence<Result> res = QueryPlus.wrap(query).run(texasRanger, responseContext);
            final Sequence<Result> results;
            if (res == null) {
              results = Sequences.empty();
            } else {
              results = res;
            }
            final Metadata headers = new Metadata();
            final Metadata trailers = new Metadata();
            call.sendHeaders(headers);
            Yielder<Result> yielder = Yielders.each(results);
            while (!yielder.isDone() && !call.isCancelled()) {
              try {
                final Result result = yielder.get();
                call.sendMessage(result);
                yielder = yielder.next(result);
              }
              catch (QueryInterruptedException ie) {
                LOG.info(ie, "Query [%s] cancelled", id);
              }
            }
            queryReceived.set(false);
            // Would it ever make sense to have one channel handle lots of queries?
            call.close(Status.OK, trailers);
          }

          public void onCancel()
          {
            if (!queryManager.cancelQuery(id)) {
              LOG.warn("Unable to cancel query [%s]", id);
            }
          }
        };
      }
    };

    @Override
    public ServerServiceDefinition bindService()
    {
      return ServerServiceDefinition
          .builder(SERVICE_DESCRIPTOR)
          .addMethod(methodDescriptor, SERVICE_CALL_HANDLER)
          .build();
    }
  }

  static final class ByteArrayInputStream extends java.io.ByteArrayInputStream implements KnownLength
  {
    ByteArrayInputStream(byte[] buf)
    {
      super(buf);
    }
  }
}
