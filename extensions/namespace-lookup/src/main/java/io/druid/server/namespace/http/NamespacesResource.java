package io.druid.server.namespace.http;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/druid/v1/namespaces")
public class NamespacesResource
{
  private static final Logger log = new Logger(NamespacesResource.class);
  private final NamespaceExtractionCacheManager namespaceExtractionCacheManager;

  @Inject
  public NamespacesResource(final NamespaceExtractionCacheManager namespaceExtractionCacheManager){
    this.namespaceExtractionCacheManager = namespaceExtractionCacheManager;
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response getNamespaces(){
    try{
      return Response.ok().entity(namespaceExtractionCacheManager.getKnownNamespaces()).build();
    }catch (Exception ex){
      log.error("Can not get the list of known namespaces");
      return Response.serverError().entity(Strings.nullToEmpty(ex.getMessage())).build();
    }
  }
}
