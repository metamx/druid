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

package io.druid.server.coordinator;

import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.util.Iterator;
import java.util.Map;

/**
 */
public class CoordinatorStats
{
  private final Map<String, Object2LongOpenHashMap<String>> perTierStats;
  private final Object2LongOpenHashMap<String> globalStats;

  private static class DefaultingObject2LongOpenHashMap<K>
      extends Object2LongOpenHashMap<K>
  {
    // to behave like DefaultingHashMap
    @Override
    public Long get(
        final Object key
    )
    {
      return getLong(key);
    }
  }

  public CoordinatorStats()
  {
    perTierStats = Maps.newHashMap();
    globalStats = new DefaultingObject2LongOpenHashMap<>();
  }

  public Map<String, ? extends Object2LongMap<String>> getPerTierStats()
  {
    return perTierStats;
  }

  public Object2LongMap<String> getGlobalStats()
  {
    return globalStats;
  }

  public void addToTieredStat(String statName, String tier, long value)
  {
    Object2LongOpenHashMap<String> theStat = perTierStats.get(statName);
    if (theStat == null) {
      theStat = new Object2LongOpenHashMap<>();
      perTierStats.put(statName, theStat);
    }
    theStat.addTo(tier, value);
  }

  public void addToGlobalStat(String statName, long value)
  {
    globalStats.addTo(statName, value);
  }

  public CoordinatorStats accumulate(CoordinatorStats stats)
  {
    for (final Map.Entry<String, Object2LongOpenHashMap<String>> entry :
        stats.perTierStats.entrySet()) {
      Object2LongOpenHashMap<String> theStat = perTierStats.get(entry.getKey());
      if (theStat == null) {
        theStat = new DefaultingObject2LongOpenHashMap<>();
        perTierStats.put(entry.getKey(), theStat);
      }

      for (final Iterator<Object2LongMap.Entry<String>> tiers =
           entry.getValue().object2LongEntrySet().fastIterator();
           tiers.hasNext(); ) {
        final Object2LongMap.Entry<String> tier = tiers.next();
        theStat.addTo(tier.getKey(), tier.getLongValue());
      }
    }

    for (final Iterator<Object2LongMap.Entry<String>> entries =
         stats.globalStats.object2LongEntrySet().fastIterator();
         entries.hasNext(); ) {
      final Object2LongMap.Entry<String> entry = entries.next();
      globalStats.addTo(entry.getKey(), entry.getLongValue());
    }
    return this;
  }
}
