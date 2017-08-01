package io.druid.grpc;

import io.grpc.ServerBuilder;
import io.grpc.ServerProvider;

public class DruidServerProvider extends ServerProvider
{
  @Override
  protected boolean isAvailable()
  {
    return true;
  }

  @Override
  protected int priority()
  {
    return 0;
  }

  @Override
  protected ServerBuilder<?> builderForPort(int port)
  {
    return new DruidServerBuilder(port);
  }
}
