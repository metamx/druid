package io.druid.grpc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.java.util.common.RE;
import io.grpc.MethodDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.ExecutorService;

public class JacksonMarshaller<T> implements MethodDescriptor.Marshaller<T>
{
  private final TypeReference<T> typeReference;
  private final ObjectMapper mapper;
  private final ExecutorService service;

  public JacksonMarshaller(ObjectMapper mapper, TypeReference<T> typeReference, ExecutorService service)
  {
    this.mapper = mapper;
    this.typeReference = typeReference;
    this.service = service;
  }

  @Override
  public InputStream stream(T value)
  {
    try {

      final PipedOutputStream os = new PipedOutputStream();
      final PipedInputStream is = new PipedInputStream(os, 1024);
      service.submit(() -> {
        try {
          mapper.writer().getFactory().createGenerator(os).writeObject(value);
        }
        catch (IOException e) {
          throw new RE(e, "crap, this will never get caught!");
        }
      });
      return is;
    }
    catch (IOException e) {
      throw new RE(e, "Error serializing %s", value);
    }
  }

  @Override
  public T parse(InputStream stream)
  {
    try {
      return mapper.readValue(stream, typeReference);
    }
    catch (IOException e) {
      throw new RE(e, "Error parsing for type %s", typeReference);
    }
  }
}
