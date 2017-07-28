package io.druid.grpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Query;
import io.druid.query.Result;
import io.grpc.BindableService;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.AbstractStub;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class QueryServer
{
  private final ObjectMapper mapper;

  @Inject
  public QueryServer(
      @Json ObjectMapper mapper
  )
  {
    this.mapper = mapper;
  }

  private static final ServiceDescriptor SERVICE_DESCRIPTOR = ServiceDescriptor.newBuilder("DruidQuery").build();
  private static final Logger LOG = new Logger(QueryServer.class);
  private final AtomicReference<Server> server = new AtomicReference<>(null);

  @LifecycleStart
  public void start()
  {
    final Server server = ServerBuilder.forPort(9987)
                                       .addService(new QueryImpl())
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
        LOG.debug("Strange race condition, not removing old server setting");
      }
      try {
        if (!server.awaitTermination(90000, TimeUnit.MILLISECONDS)) {
          throw new InterruptedException(StringUtils.safeFormat(
              "Timed out waiting for termination. Waited %d ms",
              90000
          ));
        }
      }
      catch (InterruptedException e) {
        LOG.warn(e, "Interrupted during shutdown of gRPC server, potentially unclean shutdown.");
      }
    }
  }

  class QueryImpl implements BindableService
  {
    @Override
    public ServerServiceDefinition bindService()
    {
      return ServerServiceDefinition
          .builder(SERVICE_DESCRIPTOR)
          .addMethod(
              MethodDescriptor
                  .<Query, Result>newBuilder()
                  .setFullMethodName("DruidQuery")
                  .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                  .setRequestMarshaller(
                      new MethodDescriptor.Marshaller<Query>()
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
                      }
                  )
                  .setResponseMarshaller(
                      new MethodDescriptor.Marshaller<Result>()
                      {
                        @Override
                        public InputStream stream(Result value)
                        {
                          return null;
                        }

                        @Override
                        public Result parse(InputStream stream)
                        {
                          throw new UnsupportedOperationException("Shouldn't do this");
                        }
                      }
                  )
                  // If segment version changes, might not be the same result
                  .setIdempotent(false)
                  // Limited to processing threads
                  .setSafe(false)
                  .build(),
              new ServerCallHandler<Query, Result>()
              {
                @Override
                public ServerCall.Listener<Query> startCall(
                    final ServerCall<Query, Result> call,
                    final Metadata headers
                )
                {
                  return new ServerCall.Listener<Query>()
                  {

                  };
                }
              }
          )
          .build();
    }
  }

  static final class QueryJsonStub extends AbstractStub<QueryJsonStub>
  {
    private final ObjectMapper mapper;

    protected QueryJsonStub(ObjectMapper mapper, Channel channel)
    {
      super(channel);
      this.mapper = mapper;
    }

    protected QueryJsonStub(ObjectMapper mapper, Channel channel, CallOptions callOptions)
    {
      super(channel, callOptions);
      this.mapper = mapper;
    }

    @Override
    protected QueryJsonStub build(Channel channel, CallOptions callOptions)
    {
      return new QueryJsonStub(mapper, channel, callOptions);
    }
  }
}
