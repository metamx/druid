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
package io.druid.segment.data;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteSource;

import java.io.IOException;
import java.io.InputStream;

class ByteSourceUtil
{
  public static ByteSource concat(Iterable<? extends ByteSource> sources)
  {
    return new ConcatenatedByteSource(sources);
  }

  static final class ConcatenatedByteSource extends ByteSource
  {
    private final Iterable<? extends ByteSource> sources;

    ConcatenatedByteSource(Iterable<? extends ByteSource> sources)
    {
      this.sources = Preconditions.checkNotNull(sources);
    }

    @Override
    public InputStream openStream() throws IOException
    {
      return new MultiInputStream(sources.iterator());
    }

    @Override
    public long size() throws IOException
    {
      long result = 0L;
      for (ByteSource source : sources) {
        result += source.size();
      }
      return result;
    }

    @Override
    public String toString()
    {
      return "ByteSource.concat(" + sources + ")";
    }
  }
}
