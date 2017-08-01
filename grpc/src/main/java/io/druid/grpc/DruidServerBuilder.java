package io.druid.grpc;

import com.google.common.base.Throwables;
import io.druid.java.util.common.RE;
import io.grpc.ServerStreamTracer;
import io.grpc.internal.AbstractServerImplBuilder;
import io.grpc.internal.InternalServer;
import io.grpc.internal.ServerListener;
import org.eclipse.jetty.http2.HTTP2Cipher;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.List;

public class DruidServerBuilder extends AbstractServerImplBuilder<DruidServerBuilder>
{
  private final SslContextFactory sslContextFactory = new SslContextFactory();
  private final int port;

  DruidServerBuilder(int port)
  {
    this.port = port;
  }

  @Override
  protected InternalServer buildTransportServer(List<ServerStreamTracer.Factory> streamTracerFactories)
  {
    final Server server = new Server();
    final HttpConfiguration http_config = new HttpConfiguration();
    http_config.setSecureScheme("https");
    http_config.setSecurePort(port);
    http_config.setSendXPoweredBy(false);
    final HTTP2CServerConnectionFactory http2c = new HTTP2CServerConnectionFactory(http_config);
    final HttpConfiguration https_config = new HttpConfiguration(http_config);
    https_config.addCustomizer(new SecureRequestCustomizer());
    final HTTP2ServerConnectionFactory h2 = new HTTP2ServerConnectionFactory(https_config);

    /*
    NegotiatingServerConnectionFactory.checkProtocolNegotiationAvailable();
    final ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
    alpn.setDefaultProtocol("h2");
    // SSL Connection Factory
    SslConnectionFactory ssl = new SslConnectionFactory(sslContextFactory, alpn.getProtocol());
    */

    // HTTP/2 Connector
    final ServerConnector http2Connector = new ServerConnector(
        server,
        //ssl,
        sslContextFactory,
        http2c,
        //alpn,
        h2,
        new HttpConnectionFactory(https_config)
    );
    http2Connector.setPort(port);
    server.addConnector(http2Connector);

    //ALPN.debug = false;

    return new InternalServer()
    {
      volatile ServerListener listener = null;

      @Override
      public void start(ServerListener listener) throws IOException
      {
        try {
          this.listener = listener;
          server.start();
        }
        catch (Exception e) {
          Throwables.propagateIfInstanceOf(e, IOException.class);
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void shutdown()
      {
        final ServerListener listener = this.listener;
        try {
          server.stop();
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
        finally {
          if (listener != null) {
            listener.serverShutdown();
          }
        }
      }

      @Override
      public int getPort()
      {
        return http2Connector.getPort();
      }
    };
  }

  @Override
  public DruidServerBuilder useTransportSecurity(File certChain, File privateKey)
  {
    sslContextFactory.setCipherComparator(HTTP2Cipher.COMPARATOR);
    final KeyStore keyStore;
    try {
      keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null, "nope".toCharArray());
    }
    catch (GeneralSecurityException | IOException e) {
      throw new RE(e, "Unable to load from [%s] / [%s]", certChain, privateKey);
    }
    try (OutputStream fos = new FileOutputStream("tmp.jks")) {
      keyStore.store(fos, "secret".toCharArray());
    }
    catch (IOException | GeneralSecurityException e) {
      throw new RE(e, "Unable to save temporary keystore");
    }
    sslContextFactory.setKeyStore(keyStore);
    return this;
  }
}
