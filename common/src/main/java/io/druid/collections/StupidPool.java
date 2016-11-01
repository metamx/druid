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

package io.druid.collections;

import com.google.common.base.Supplier;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import sun.misc.Cleaner;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class StupidPool<T>
{
  private static final Logger log = new Logger(StupidPool.class);

  private final Supplier<T> generator;

  private final Queue<T> objects = new ConcurrentLinkedQueue<>();
  /**
   * {@link ConcurrentLinkedQueue}'s size() is O(n) queue traversal apparently for the sake of being 100%
   * wait-free, that is not required by {@code StupidPool}. In {@code poolSize} we account the queue size
   * ourselves, to avoid traversal of {@link #objects} in {@link #tryReturnToPool(Object)}.
   */
  private final AtomicLong poolSize = new AtomicLong(0);

  //note that this is just the max entries in the cache, pool can still create as many buffers as needed.
  private final int objectsCacheMaxCount;

  public StupidPool(
      Supplier<T> generator
  )
  {
    this.generator = generator;
    this.objectsCacheMaxCount = Integer.MAX_VALUE;
  }

  public StupidPool(
      Supplier<T> generator,
      int objectsCacheMaxCount
  )
  {
    this.generator = generator;
    this.objectsCacheMaxCount = objectsCacheMaxCount;
  }

  public ResourceHolder<T> take()
  {
    final T obj = objects.poll();
    if (obj == null) {
      return new ObjectResourceHolder(generator.get());
    } else {
      poolSize.decrementAndGet();
      return new ObjectResourceHolder(obj);
    }
  }

  /** For tests */
  long poolSize() {
    return poolSize.get();
  }

  private void tryReturnToPool(T object)
  {
    long currentPoolSize;
    do {
      currentPoolSize = poolSize.get();
      if (currentPoolSize >= objectsCacheMaxCount) {
        log.debug("cache num entries is exceeding max limit [%s]", objectsCacheMaxCount);
        return;
      }
    } while (!poolSize.compareAndSet(currentPoolSize, currentPoolSize + 1));
    if (!objects.offer(object)) {
      poolSize.decrementAndGet();
      log.warn(new ISE("Queue offer failed"), "Could not offer object [%s] back into the queue", object);
    }
  }

  private class ObjectResourceHolder implements ResourceHolder<T>
  {
    private final AtomicReference<T> objectRef;
    private final Cleaner cleaner;

    ObjectResourceHolder(final T object)
    {
      this.objectRef = new AtomicReference<>(object);
      this.cleaner = Cleaner.create(ObjectResourceHolder.this, new ObjectReclaimer(objectRef));
    }

    // WARNING: it is entirely possible for a caller to hold onto the object and call ObjectResourceHolder.close,
    // Then still use that object even though it will be offered to someone else in StupidPool.take
    @Override
    public T get()
    {
      final T object = objectRef.get();
      if (object == null) {
        throw new ISE("Already Closed!");
      }

      return object;
    }

    @Override
    public void close()
    {
      final T object = objectRef.get();
      if (object != null && objectRef.compareAndSet(object, null)) {
        tryReturnToPool(object);
        // Effectively does nothing, because objectRef is already set to null. The purpose of this call is to
        // deregister the cleaner from the internal linked list of all cleaners in the JVM.
        cleaner.clean();
      } else {
        log.warn(new ISE("Already Closed!"), "Already closed");
      }
    }
  }

  private class ObjectReclaimer implements Runnable
  {
    private final AtomicReference<T> objectRef;

    private ObjectReclaimer(AtomicReference<T> objectRef)
    {
      this.objectRef = objectRef;
    }

    @Override
    public void run()
    {
      try {
        final T object = objectRef.get();
        if (object != null && objectRef.compareAndSet(object, null)) {
          log.warn("Not closed!  Object was[%s]. Allowing gc to prevent leak.", object);
          tryReturnToPool(object);
        }
      }
      // Exceptions must not be thrown in Cleaner.clean(), which calls this ObjectReclaimer.run() method
      catch (Exception e) {
        try {
          log.error(e, "Exception in ObjectReclaimer.run()");
        }
        catch (Exception ignore) {
          // ignore
        }
      }
    }
  }
}
