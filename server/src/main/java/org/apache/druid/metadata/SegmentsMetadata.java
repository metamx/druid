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

package org.apache.druid.metadata;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * The difference between this class and {@link org.apache.druid.sql.calcite.schema.MetadataSegmentView} is that this
 * class resides in Coordinator's memory, while {@link org.apache.druid.sql.calcite.schema.MetadataSegmentView} resides
 * in Broker's memory.
 */
public interface SegmentsMetadata
{
  void start();

  void stop();

  boolean tryMarkAsUsedAllSegmentsInDataSource(String dataSource);

  boolean tryMarkSegmentAsUsed(String segmentId);

  boolean tryMarkAsUnusedAllSegmentsInDataSource(String dataSource);

  /**
   * Prefer {@link #tryMarkSegmentAsUnused(SegmentId)} to this method when possible.
   *
   * This method is not removed because {@link org.apache.druid.server.http.DataSourcesResource#removeSegment}
   * uses it and if it migrates to {@link #tryMarkSegmentAsUnused(SegmentId)} the performance will be worse.
   */
  boolean tryMarkSegmentAsUnused(String dataSource, String segmentId);

  boolean tryMarkSegmentAsUnused(SegmentId segmentId);

  boolean isStarted();

  /**
   * If there are used segments belonging to the given data source, this method converts this set of segments to an
   * {@link ImmutableDruidDataSource} object and returns. If there are no used segments belonging to the given data
   * source, this method returns null.
   *
   * This method's name starts with "prepare" to indicate that it's not cheap (it creates an {@link
   * ImmutableDruidDataSource} object). Not used "create" prefix to avoid giving a false impression that this method
   * might put something into the database to create a data source with the given name, if absent.
   */
  @Nullable
  ImmutableDruidDataSource prepareImmutableDataSourceWithUsedSegments(String dataSource);

  /**
   * If there are used segments belonging to the given data source, this method returns a {@link DruidDataSource} object
   * with a view on those segments. If there are no used segments belonging to the given data source, this method
   * returns null.
   *
   * Note that the returned {@link DruidDataSource} object may be updated concurrently and already be empty by the time
   * it is returned.
   */
  @Nullable
  DruidDataSource getDataSourceWithUsedSegments(String dataSource);

  /**
   * Prepares a set of {@link ImmutableDruidDataSource} objects containing information about all used segments. {@link
   * ImmutableDruidDataSource} objects in the returned collection are unique. If there are no used segments, this method
   * returns an empty collection.
   *
   * This method's name starts with "prepare" for the same reason as {@link
   * #prepareImmutableDataSourceWithUsedSegments}.
   */
  Collection<ImmutableDruidDataSource> prepareImmutableDataSourcesWithAllUsedSegments();

  /**
   * Returns an iterable to go over all segments in all data sources. The order in which segments are iterated is
   * unspecified. Note: the iteration may not be as trivially cheap as, for example, iteration over an ArrayList. Try
   * (to some reasonable extent) to organize the code so that it iterates the returned iterable only once rather than
   * several times.
   */
  Iterable<DataSegment> iterateAllUsedSegments();

  /**
   * Retrieves all data source names for which there are segment in the database, regardless of whether those segments
   * are used or not. Data source names in the returned collection are unique. If there are no segments in the database,
   * returns an empty collection.
   *
   * Performance warning: this method makes a query into the database.
   *
   * This method might return a different set of data source names than may be observed via {@link
   * #prepareImmutableDataSourcesWithAllUsedSegments} method. This method will include a data source name even if there
   * are no used segments belonging to it, while {@link #prepareImmutableDataSourcesWithAllUsedSegments} won't return
   * such a data source.
   */
  Collection<String> retrieveAllDataSourceNames();

  /**
   * Returns top N unused segment intervals with the end time no later than the specified maxEndTime when ordered by
   * segment start time, end time.
   */
  List<Interval> getUnusedSegmentIntervals(String dataSource, DateTime maxEndTime, int limit);

  @VisibleForTesting
  void poll();
}