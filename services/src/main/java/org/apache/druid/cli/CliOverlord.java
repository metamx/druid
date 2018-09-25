/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.util.Providers;
import io.airlift.airline.Command;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.client.indexing.HttpIndexingServiceClient;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.IndexingServiceSelectorConfig;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.guice.IndexingServiceFirehoseModule;
import org.apache.druid.guice.IndexingServiceModuleHelper;
import org.apache.druid.guice.IndexingServiceTaskLogsModule;
import org.apache.druid.guice.JacksonConfigProvider;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.ListProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.common.actions.LocalTaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionToolbox;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTaskClient;
import org.apache.druid.indexing.common.tasklogs.SwitchingTaskLogStreamer;
import org.apache.druid.indexing.common.tasklogs.TaskRunnerTaskLogStreamer;
import org.apache.druid.indexing.overlord.ForkingTaskRunnerFactory;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageAdapter;
import org.apache.druid.indexing.overlord.MetadataTaskStorage;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.TaskStorageQueryAdapter;
import org.apache.druid.indexing.overlord.autoscaling.PendingTaskBasedWorkerProvisioningConfig;
import org.apache.druid.indexing.overlord.autoscaling.PendingTaskBasedWorkerProvisioningStrategy;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningSchedulerConfig;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningStrategy;
import org.apache.druid.indexing.overlord.autoscaling.SimpleWorkerProvisioningConfig;
import org.apache.druid.indexing.overlord.autoscaling.SimpleWorkerProvisioningStrategy;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.overlord.helpers.OverlordHelper;
import org.apache.druid.indexing.overlord.helpers.TaskLogAutoCleaner;
import org.apache.druid.indexing.overlord.helpers.TaskLogAutoCleanerConfig;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerResource;
import org.apache.druid.indexing.overlord.http.OverlordRedirectInfo;
import org.apache.druid.indexing.overlord.http.OverlordResource;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorResource;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.audit.AuditManagerProvider;
import org.apache.druid.server.coordinator.CoordinatorOverlordServiceConfig;
import org.apache.druid.server.http.RedirectFilter;
import org.apache.druid.server.http.RedirectInfo;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.jetty.JettyServerInitUtils;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationUtils;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.tasklogs.TaskLogStreamer;
import org.apache.druid.tasklogs.TaskLogs;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.ResourceCollection;

import java.util.List;

/**
 */
@Command(
    name = "overlord",
    description = "Runs an Overlord node, see http://druid.io/docs/latest/Indexing-Service.html for a description"
)
public class CliOverlord extends ServerRunnable
{
  private static Logger log = new Logger(CliOverlord.class);

  protected static List<String> UNSECURED_PATHS = ImmutableList.of(
      "/",
      "/console.html",
      "/old-console/*",
      "/images/*",
      "/js/*",
      "/druid/indexer/v1/isLeader",
      "/status/health"
  );

  public CliOverlord()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return getModules(true);
  }

  protected List<? extends Module> getModules(final boolean standalone)
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            if (standalone) {
              binder.bindConstant()
                    .annotatedWith(Names.named("serviceName"))
                    .to(IndexingServiceSelectorConfig.DEFAULT_SERVICE_NAME);
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8090);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8290);
            }

            JsonConfigProvider.bind(binder, "druid.coordinator.asOverlord", CoordinatorOverlordServiceConfig.class);
            JsonConfigProvider.bind(binder, "druid.indexer.queue", TaskQueueConfig.class);
            JsonConfigProvider.bind(binder, "druid.indexer.task", TaskConfig.class);

            binder.bind(TaskMaster.class).in(ManageLifecycle.class);

            binder.bind(TaskLogStreamer.class).to(SwitchingTaskLogStreamer.class).in(LazySingleton.class);
            binder.bind(
                new TypeLiteral<List<TaskLogStreamer>>()
                {
                }
            )
                  .toProvider(
                      new ListProvider<TaskLogStreamer>()
                          .add(TaskRunnerTaskLogStreamer.class)
                          .add(TaskLogs.class)
                  )
                  .in(LazySingleton.class);

            binder.bind(TaskActionClientFactory.class).to(LocalTaskActionClientFactory.class).in(LazySingleton.class);
            binder.bind(TaskActionToolbox.class).in(LazySingleton.class);
            binder.bind(TaskLockbox.class).in(LazySingleton.class);
            binder.bind(TaskStorageQueryAdapter.class).in(LazySingleton.class);
            binder.bind(IndexerMetadataStorageAdapter.class).in(LazySingleton.class);
            binder.bind(SupervisorManager.class).in(LazySingleton.class);

            binder.bind(IndexingServiceClient.class).to(HttpIndexingServiceClient.class).in(LazySingleton.class);
            binder.bind(new TypeLiteral<IndexTaskClientFactory<ParallelIndexTaskClient>>(){})
                  .toProvider(Providers.of(null));
            binder.bind(ChatHandlerProvider.class).toProvider(Providers.of(null));

            PolyBind.createChoice(
                binder,
                "druid.indexer.task.rowIngestionMeters.type",
                Key.get(RowIngestionMetersFactory.class),
                Key.get(DropwizardRowIngestionMetersFactory.class)
            );
            final MapBinder<String, RowIngestionMetersFactory> rowIngestionMetersHandlerProviderBinder = PolyBind.optionBinder(
                binder, Key.get(RowIngestionMetersFactory.class)
            );
            rowIngestionMetersHandlerProviderBinder.addBinding("dropwizard")
                                                   .to(DropwizardRowIngestionMetersFactory.class).in(LazySingleton.class);
            binder.bind(DropwizardRowIngestionMetersFactory.class).in(LazySingleton.class);

            configureTaskStorage(binder);
            configureAutoscale(binder);
            configureRunners(binder);
            configureOverlordHelpers(binder);

            binder.bind(AuditManager.class)
                  .toProvider(AuditManagerProvider.class)
                  .in(ManageLifecycle.class);

            if (standalone) {
              binder.bind(RedirectFilter.class).in(LazySingleton.class);
              binder.bind(RedirectInfo.class).to(OverlordRedirectInfo.class).in(LazySingleton.class);
              binder.bind(JettyServerInitializer.class)
                    .to(OverlordJettyServerInitializer.class)
                    .in(LazySingleton.class);
            }

            Jerseys.addResource(binder, OverlordResource.class);
            Jerseys.addResource(binder, SupervisorResource.class);
            Jerseys.addResource(binder, HttpRemoteTaskRunnerResource.class);

            if (standalone) {
              LifecycleModule.register(binder, Server.class);
            }

            binder
                .bind(DiscoverySideEffectsProvider.Child.class)
                .annotatedWith(IndexingService.class)
                .toProvider(new DiscoverySideEffectsProvider(NodeType.overlord, ImmutableList.of()))
                .in(LazySingleton.class);
            LifecycleModule.registerKey(binder, Key.get(DiscoverySideEffectsProvider.Child.class, IndexingService.class));
          }

          private void configureTaskStorage(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.indexer.storage", TaskStorageConfig.class);

            PolyBind.createChoice(
                binder, "druid.indexer.storage.type", Key.get(TaskStorage.class), Key.get(HeapMemoryTaskStorage.class)
            );
            final MapBinder<String, TaskStorage> storageBinder = PolyBind.optionBinder(
                binder,
                Key.get(TaskStorage.class)
            );

            storageBinder.addBinding("local").to(HeapMemoryTaskStorage.class);
            binder.bind(HeapMemoryTaskStorage.class).in(LazySingleton.class);

            storageBinder.addBinding("metadata").to(MetadataTaskStorage.class).in(ManageLifecycle.class);
            binder.bind(MetadataTaskStorage.class).in(LazySingleton.class);
          }

          private void configureRunners(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.worker", WorkerConfig.class);

            PolyBind.createChoice(
                binder,
                "druid.indexer.runner.type",
                Key.get(TaskRunnerFactory.class),
                Key.get(ForkingTaskRunnerFactory.class)
            );
            final MapBinder<String, TaskRunnerFactory> biddy = PolyBind.optionBinder(
                binder,
                Key.get(TaskRunnerFactory.class)
            );

            IndexingServiceModuleHelper.configureTaskRunnerConfigs(binder);
            biddy.addBinding("local").to(ForkingTaskRunnerFactory.class);
            binder.bind(ForkingTaskRunnerFactory.class).in(LazySingleton.class);

            biddy.addBinding(RemoteTaskRunnerFactory.TYPE_NAME).to(RemoteTaskRunnerFactory.class).in(LazySingleton.class);
            binder.bind(RemoteTaskRunnerFactory.class).in(LazySingleton.class);

            biddy.addBinding(HttpRemoteTaskRunnerFactory.TYPE_NAME).to(HttpRemoteTaskRunnerFactory.class).in(LazySingleton.class);
            binder.bind(HttpRemoteTaskRunnerFactory.class).in(LazySingleton.class);

            JacksonConfigProvider.bind(binder, WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class, null);
          }

          private void configureAutoscale(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.indexer.autoscale", ProvisioningSchedulerConfig.class);
            JsonConfigProvider.bind(
                binder,
                "druid.indexer.autoscale",
                PendingTaskBasedWorkerProvisioningConfig.class
            );
            JsonConfigProvider.bind(binder, "druid.indexer.autoscale", SimpleWorkerProvisioningConfig.class);

            PolyBind.createChoice(
                binder,
                "druid.indexer.autoscale.strategy.type",
                Key.get(ProvisioningStrategy.class),
                Key.get(SimpleWorkerProvisioningStrategy.class)
            );
            final MapBinder<String, ProvisioningStrategy> biddy = PolyBind.optionBinder(
                binder,
                Key.get(ProvisioningStrategy.class)
            );
            biddy.addBinding("simple").to(SimpleWorkerProvisioningStrategy.class);
            biddy.addBinding("pendingTaskBased").to(PendingTaskBasedWorkerProvisioningStrategy.class);
          }

          private void configureOverlordHelpers(Binder binder)
          {
            JsonConfigProvider.bind(binder, "druid.indexer.logs.kill", TaskLogAutoCleanerConfig.class);
            Multibinder.newSetBinder(binder, OverlordHelper.class)
                       .addBinding()
                       .to(TaskLogAutoCleaner.class);
          }
        },
        new IndexingServiceFirehoseModule(),
        new IndexingServiceTaskLogsModule()
    );
  }

  /**
   */
  private static class OverlordJettyServerInitializer implements JettyServerInitializer
  {
    private final AuthConfig authConfig;
    private final ServerConfig serverConfig;

    @Inject
    OverlordJettyServerInitializer(AuthConfig authConfig, ServerConfig serverConfig)
    {
      this.authConfig = authConfig;
      this.serverConfig = serverConfig;
    }

    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");
      root.setInitParameter("org.eclipse.jetty.servlet.Default.redirectWelcome", "true");
      root.setWelcomeFiles(new String[]{"index.html", "console.html"});

      ServletHolder holderPwd = new ServletHolder("default", DefaultServlet.class);

      root.addServlet(holderPwd, "/");
      root.setBaseResource(
          new ResourceCollection(
              new String[]{
                  TaskMaster.class.getClassLoader().getResource("static").toExternalForm(),
                  TaskMaster.class.getClassLoader().getResource("indexer_static").toExternalForm()
              }
          )
      );

      final ObjectMapper jsonMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
      final AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

      AuthenticationUtils.addSecuritySanityCheckFilter(root, jsonMapper);

      // perform no-op authorization for these resources
      AuthenticationUtils.addNoopAuthorizationFilters(root, UNSECURED_PATHS);
      AuthenticationUtils.addNoopAuthorizationFilters(root, authConfig.getUnsecuredPaths());

      final List<Authenticator> authenticators = authenticatorMapper.getAuthenticatorChain();
      AuthenticationUtils.addAuthenticationFilterChain(root, authenticators);

      AuthenticationUtils.addAllowOptionsFilter(root, authConfig.isAllowUnauthenticatedHttpOptions());

      JettyServerInitUtils.addExtensionFilters(root, injector);


      // Check that requests were authorized before sending responses
      AuthenticationUtils.addPreResponseAuthorizationCheckFilter(
          root,
          authenticators,
          jsonMapper
      );

      // add some paths not to be redirected to leader.
      root.addFilter(GuiceFilter.class, "/status/*", null);
      root.addFilter(GuiceFilter.class, "/druid-internal/*", null);

      // redirect anything other than status to the current lead
      root.addFilter(new FilterHolder(injector.getInstance(RedirectFilter.class)), "/*", null);

      // Can't use /* here because of Guice and Jetty static content conflicts
      root.addFilter(GuiceFilter.class, "/druid/*", null);

      root.addFilter(GuiceFilter.class, "/druid-ext/*", null);

      HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(
          new Handler[]{
              JettyServerInitUtils.getJettyRequestLogHandler(),
              JettyServerInitUtils.wrapWithDefaultGzipHandler(
                  root,
                  serverConfig.getInflateBufferSize(),
                  serverConfig.getCompressionLevel()
              )
          }
      );

      server.setHandler(handlerList);
    }
  }
}
