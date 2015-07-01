package io.druid.server.initialization;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.druid.query.extraction.namespace.ExtractionNamespace;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class NamespaceLookupStaticConfig
{
  @JsonProperty ExtractionNamespace[] lookups;

  public List<ExtractionNamespace> getNamespaces()
  {
    return lookups == null ? Collections.<ExtractionNamespace>emptyList() : Arrays.asList(lookups);
  }
}
