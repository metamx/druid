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

package io.druid.segment;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import io.druid.granularity.QueryGranularity;
import io.druid.query.BaseQuery;
import io.druid.query.QueryMetricsContext;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapResult;
import io.druid.query.filter.BooleanFilter;
import io.druid.query.filter.DruidLongPredicate;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.Filter;
import io.druid.query.filter.RowOffsetMatcherFactory;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatcherFactory;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.Offset;
import io.druid.segment.filter.AndFilter;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.filter.Filters;
import io.druid.segment.historical.HistoricalCursor;
import io.druid.segment.historical.HistoricalDimensionSelector;
import io.druid.segment.historical.HistoricalFloatColumnSelector;
import io.druid.segment.historical.HistoricalLongColumnSelector;
import io.druid.segment.historical.HistoricalSingleScanTimeDimSelector;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.roaringbitmap.IntIterator;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
public class QueryableIndexStorageAdapter implements StorageAdapter
{
  private static final Logger log = new Logger(QueryableIndexStorageAdapter.class);
  private static final NullDimensionSelector NULL_DIMENSION_SELECTOR = new NullDimensionSelector();

  private final QueryableIndex index;

  public QueryableIndexStorageAdapter(
      QueryableIndex index
  )
  {
    this.index = index;
  }

  @Override
  public String getSegmentIdentifier()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Interval getInterval()
  {
    return index.getDataInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return index.getAvailableDimensions();
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return Sets.difference(Sets.newHashSet(index.getColumnNames()), Sets.newHashSet(index.getAvailableDimensions()));
  }

  @Override
  public int getDimensionCardinality(String dimension)
  {
    if (dimension == null) {
      return 0;
    }

    Column column = index.getColumn(dimension);
    if (column == null) {
      return 0;
    }
    if (!column.getCapabilities().isDictionaryEncoded()) {
      return Integer.MAX_VALUE;
    }
    return column.getDictionaryEncoding().getCardinality();
  }

  @Override
  public int getNumRows()
  {
    return index.getNumRows();
  }

  @Override
  public DateTime getMinTime()
  {
    try (final GenericColumn column = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn()) {
      return new DateTime(column.getLongSingleValueRow(0));
    }
  }

  @Override
  public DateTime getMaxTime()
  {
    try (final GenericColumn column = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn()) {
      return new DateTime(column.getLongSingleValueRow(column.length() - 1));
    }
  }

  @Override
  public Comparable getMinValue(String dimension)
  {
    Column column = index.getColumn(dimension);
    if (column != null && column.getCapabilities().hasBitmapIndexes()) {
      BitmapIndex bitmap = column.getBitmapIndex();
      return bitmap.getCardinality() > 0 ? bitmap.getValue(0) : null;
    }
    return null;
  }

  @Override
  public Comparable getMaxValue(String dimension)
  {
    Column column = index.getColumn(dimension);
    if (column != null && column.getCapabilities().hasBitmapIndexes()) {
      BitmapIndex bitmap = column.getBitmapIndex();
      return bitmap.getCardinality() > 0 ? bitmap.getValue(bitmap.getCardinality() - 1) : null;
    }
    return null;
  }

  @Override
  public Capabilities getCapabilities()
  {
    return Capabilities.builder().dimensionValuesSorted(true).build();
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return getColumnCapabilites(index, column);
  }

  @Override
  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    return index.getDimensionHandlers();
  }

  @Override
  public String getColumnTypeName(String columnName)
  {
    final Column column = index.getColumn(columnName);
    try (final ComplexColumn complexColumn = column.getComplexColumn()) {
      return complexColumn != null ? complexColumn.getTypeName() : column.getCapabilities().getType().toString();
    }
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    // For immutable indexes, maxIngestedEventTime is maxTime.
    return getMaxTime();
  }

  @Override
  public Sequence<Cursor> makeCursors(
      Filter filter,
      Interval interval,
      QueryGranularity gran,
      boolean descending,
      @Nullable QueryMetricsContext queryMetricsContext
  )
  {
    Interval actualInterval = interval;

    long minDataTimestamp = getMinTime().getMillis();
    long maxDataTimestamp = getMaxTime().getMillis();
    final Interval dataInterval = new Interval(
        minDataTimestamp,
        gran.next(gran.truncate(maxDataTimestamp))
    );

    if (!actualInterval.overlaps(dataInterval)) {
      return Sequences.empty();
    }

    if (actualInterval.getStart().isBefore(dataInterval.getStart())) {
      actualInterval = actualInterval.withStart(dataInterval.getStart());
    }
    if (actualInterval.getEnd().isAfter(dataInterval.getEnd())) {
      actualInterval = actualInterval.withEnd(dataInterval.getEnd());
    }

    final ColumnSelectorBitmapIndexSelector selector = new ColumnSelectorBitmapIndexSelector(
        index.getBitmapFactoryForDimensions(),
        index
    );

    int totalRows = index.getNumRows();

    /**
     * Filters can be applied in two stages:
     * pre-filtering: Use bitmap indexes to prune the set of rows to be scanned.
     * post-filtering: Iterate through rows and apply the filter to the row values
     *
     * The pre-filter and post-filter step have an implicit AND relationship. (i.e., final rows are those that
     * were not pruned AND those that matched the filter during row scanning)
     *
     * An AND filter can have its subfilters partitioned across the two steps. The subfilters that can be
     * processed entirely with bitmap indexes (subfilter returns true for supportsBitmapIndex())
     * will be moved to the pre-filtering stage.
     *
     * Any subfilters that cannot be processed entirely with bitmap indexes will be moved to the post-filtering stage.
     */
    final Offset offset;
    final List<Filter> postFilters = new ArrayList<>();
    List<String> bitmapConstructionSpecs = null;
    int numPreFilters = 0;
    long bitmapFilteredRows = totalRows;
    long bitmapIntersectionTimeNs = 0L;
    if (filter == null) {
      offset = new NoFilterOffset(0, totalRows, descending);
    } else {
      final List<Filter> preFilters = new ArrayList<>();

      if (filter instanceof AndFilter) {
        // If we get an AndFilter, we can split the subfilters across both filtering stages
        for (Filter subfilter : ((AndFilter) filter).getFilters()) {
          if (subfilter.supportsBitmapIndex(selector)) {
            preFilters.add(subfilter);
          } else {
            postFilters.add(subfilter);
          }
        }
      } else {
        // If we get an OrFilter or a single filter, handle the filter in one stage
        if (filter.supportsBitmapIndex(selector)) {
          preFilters.add(filter);
        } else {
          postFilters.add(filter);
        }
      }
      numPreFilters = preFilters.size();

      if (preFilters.size() == 0) {
        offset = new NoFilterOffset(0, totalRows, descending);
      } else {
        bitmapConstructionSpecs = new ArrayList<>();
        List<ImmutableBitmap> bitmaps = Lists.newArrayList();
        for (Filter prefilter : preFilters) {
          BitmapResult bitmapResult = prefilter.getBitmapIndex(selector);
          bitmaps.add(bitmapResult.getBitmap());
          bitmapConstructionSpecs.add(bitmapResult.getConstructionSpecification());
        }
        final long bitmapIntersectionStartTimeNs = queryMetricsContext != null ? System.nanoTime() : 0;
        ImmutableBitmap intersectionBitmap;
        if (bitmaps.size() > 1) {
          intersectionBitmap = selector.getBitmapFactory().intersection(bitmaps);
        } else {
          intersectionBitmap = bitmaps.get(0);
        }
        bitmapFilteredRows = intersectionBitmap.size();
        if (queryMetricsContext != null) {
          bitmapIntersectionTimeNs = System.nanoTime() - bitmapIntersectionStartTimeNs;
        }
        offset = BitmapOffset.of(
            intersectionBitmap,
            descending,
            bitmapFilteredRows / (double) totalRows
        );
      }
    }

    final Filter postFilter;
    if (postFilters.size() == 0) {
      postFilter = null;
    } else if (postFilters.size() == 1) {
      postFilter = postFilters.get(0);
    } else {
      postFilter = new AndFilter(postFilters);
    }

    if (queryMetricsContext != null) {
      queryMetricsContext.metrics.put("query/totalRows", totalRows);
      queryMetricsContext.metrics.put("query/bitmapFilteredRows", bitmapFilteredRows);
      queryMetricsContext.metrics.put("query/bitmapIntersectionTimeNs", bitmapIntersectionTimeNs);
      queryMetricsContext.setDimension("numPreFilters", numPreFilters);
      queryMetricsContext.setDimension("numPostFilters", postFilters.size());
      queryMetricsContext.multiValueDimensions.put("postFilterClassNames", postFilterClassNames(postFilters));
      if (bitmapConstructionSpecs != null) {
        String[] specs = bitmapConstructionSpecs.toArray(new String[0]);
        queryMetricsContext.multiValueDimensions.put("bitmapConstructionSpecs", specs);
      }
      log.debug("TopN filters: %s", postFilters);
    }

    return Sequences.filter(
        new CursorSequenceBuilder(
            index,
            actualInterval,
            gran,
            offset,
            minDataTimestamp,
            maxDataTimestamp,
            descending,
            postFilter,
            selector
        ).build(),
        Predicates.<Cursor>notNull()
    );
  }

  private static String[] postFilterClassNames(List<Filter> postFilters)
  {
    String[] postFilterClassNames = new String[postFilters.size()];
    for (int i = 0; i < postFilters.size(); i++) {
      postFilterClassNames[i] = postFilters.get(i).getClass().getName();
    }
    return postFilterClassNames;
  }

  private static ColumnCapabilities getColumnCapabilites(ColumnSelector index, String columnName)
  {
    Column columnObj = index.getColumn(columnName);
    if (columnObj == null) {
      return null;
    }
    return columnObj.getCapabilities();
  }

  private static class CursorSequenceBuilder
  {
    private final ColumnSelector index;
    private final Interval interval;
    private final QueryGranularity gran;
    private final Offset offset;
    private final long minDataTimestamp;
    private final long maxDataTimestamp;
    private final boolean descending;
    private final Filter postFilter;
    private final ColumnSelectorBitmapIndexSelector bitmapIndexSelector;

    public CursorSequenceBuilder(
        ColumnSelector index,
        Interval interval,
        QueryGranularity gran,
        Offset offset,
        long minDataTimestamp,
        long maxDataTimestamp,
        boolean descending,
        Filter postFilter,
        ColumnSelectorBitmapIndexSelector bitmapIndexSelector
    )
    {
      this.index = index;
      this.interval = interval;
      this.gran = gran;
      this.offset = offset;
      this.minDataTimestamp = minDataTimestamp;
      this.maxDataTimestamp = maxDataTimestamp;
      this.descending = descending;
      this.postFilter = postFilter;
      this.bitmapIndexSelector = bitmapIndexSelector;
    }

    public Sequence<Cursor> build()
    {
      final Offset baseOffset = offset.clone();

      final Map<String, DictionaryEncodedColumn> dictionaryColumnCache = Maps.newHashMap();
      final Map<String, GenericColumn> genericColumnCache = Maps.newHashMap();
      final Map<String, Object> objectColumnCache = Maps.newHashMap();

      final GenericColumn timestamps = index.getColumn(Column.TIME_COLUMN_NAME).getGenericColumn();

      final Closer closer = Closer.create();
      closer.register(timestamps);

      Iterable<Long> iterable = gran.iterable(interval.getStartMillis(), interval.getEndMillis());
      if (descending) {
        iterable = Lists.reverse(ImmutableList.copyOf(iterable));
      }

      return Sequences.withBaggage(
          Sequences.map(
              Sequences.simple(iterable),
              new Function<Long, Cursor>()
              {
                @Override
                public Cursor apply(final Long input)
                {
                  final long timeStart = Math.max(interval.getStartMillis(), input);
                  final long timeEnd = Math.min(interval.getEndMillis(), gran.next(input));

                  if (descending) {
                    for (; baseOffset.withinBounds(); baseOffset.increment()) {
                      if (timestamps.getLongSingleValueRow(baseOffset.getOffset()) < timeEnd) {
                        break;
                      }
                    }
                  } else {
                    for (; baseOffset.withinBounds(); baseOffset.increment()) {
                      if (timestamps.getLongSingleValueRow(baseOffset.getOffset()) >= timeStart) {
                        break;
                      }
                    }
                  }

                  final Offset initOffset = baseOffset.clone();
                  final DateTime myBucket = gran.toDateTime(input);

                  abstract class QueryableIndexBaseCursor implements Cursor
                  {
                    Offset cursorOffset;
                    final CursorOffsetHolder cursorOffsetHolder = new CursorOffsetHolder();

                    public Offset copyOffset()
                    {
                      return cursorOffset.clone();
                    }

                    Offset resetOffset()
                    {
                      if (descending) {
                        if (minDataTimestamp >= timeStart) {
                          return initOffset.clone();
                        } else {
                          return new DescendingTimestampCheckingOffset(
                              initOffset.clone(),
                              timestamps,
                              timeStart
                          );
                        }
                      } else {
                        if (maxDataTimestamp < timeEnd) {
                          return initOffset.clone();
                        } else {
                          return new AscendingTimestampCheckingOffset(
                              initOffset.clone(),
                              timestamps,
                              timeEnd
                          );
                        }
                      }
                    }

                    public HistoricalDimensionSelector makeDimensionSelector(
                        DimensionSpec dimensionSpec
                    )
                    {
                      return dimensionSpec.decorateHistorical(makeDimensionSelectorUndecorated(dimensionSpec));
                    }

                    private HistoricalDimensionSelector makeDimensionSelectorUndecorated(
                        DimensionSpec dimensionSpec
                    )
                    {
                      final String dimension = dimensionSpec.getDimension();
                      final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

                      final Column columnDesc = index.getColumn(dimension);
                      if (columnDesc == null) {
                        return NULL_DIMENSION_SELECTOR;
                      }

                      if (dimension.equals(Column.TIME_COLUMN_NAME)) {
                        return new HistoricalSingleScanTimeDimSelector(
                            makeLongColumnSelector(dimension),
                            extractionFn,
                            descending
                        );
                      }

                      DictionaryEncodedColumn<String> cachedColumn = dictionaryColumnCache.get(dimension);
                      if (cachedColumn == null) {
                        cachedColumn = columnDesc.getDictionaryEncoding();
                        closer.register(cachedColumn);
                        dictionaryColumnCache.put(dimension, cachedColumn);
                      }

                      final DictionaryEncodedColumn<String> column = cachedColumn;

                      if (column == null) {
                        return NULL_DIMENSION_SELECTOR;
                      } else if (columnDesc.getCapabilities().hasMultipleValues()) {
                        class MultiValueDimensionSelector implements HistoricalDimensionSelector {
                          @Override
                          public IndexedInts getRow()
                          {
                            return column.getMultiValueRow(cursorOffset.getOffset());
                          }

                          @Override
                          public IndexedInts getRow(int rowNum)
                          {
                            return column.getMultiValueRow(rowNum);
                          }

                          @Override
                          public int constantRowSize()
                          {
                            return VARIABLE_ROW_SIZE;
                          }

                          @Override
                          public int getValueCardinality()
                          {
                            return column.getCardinality();
                          }

                          @Override
                          public String lookupName(int id)
                          {
                            final String value = column.lookupName(id);
                            return extractionFn == null ?
                                   value :
                                   extractionFn.apply(value);
                          }

                          @Override
                          public int lookupId(String name)
                          {
                            if (extractionFn != null) {
                              throw new UnsupportedOperationException(
                                  "cannot perform lookup when applying an extraction function"
                              );
                            }
                            return column.lookupId(name);
                          }

                          @Override
                          public String getDimensionSelectorType()
                          {
                            return getClass().getName() + "["
                                   + "column=" + column.getDictionaryEncodedColumnType()
                                   + ", cursorOffset=" + cursorOffset.getOffsetType()
                                   + "]";
                          }
                        }
                        return new MultiValueDimensionSelector();
                      } else {
                        return column.makeHistoricalDimensionSelector(cursorOffsetHolder, extractionFn);
                      }
                    }

                    public HistoricalFloatColumnSelector makeFloatColumnSelector(String columnName)
                    {
                      GenericColumn cachedMetricVals = getCachedMetricVals(columnName);

                      if (cachedMetricVals == null) {
                        return FloatZeroSelector.singleton();
                      }

                      return cachedMetricVals.makeHistoricalColumnFloatSelector(cursorOffsetHolder);
                    }

                    public HistoricalLongColumnSelector makeLongColumnSelector(String columnName)
                    {
                      GenericColumn cachedMetricVals = genericColumnCache.get(columnName);

                      if (cachedMetricVals == null) {
                        Column holder = index.getColumn(columnName);
                        if (holder != null && (holder.getCapabilities().getType() == ValueType.LONG
                                               || holder.getCapabilities().getType() == ValueType.FLOAT)) {
                          cachedMetricVals = holder.getGenericColumn();
                          closer.register(cachedMetricVals);
                          genericColumnCache.put(columnName, cachedMetricVals);
                        }
                      }

                      if (cachedMetricVals == null) {
                        return LongZeroSelector.singleton();
                      }

                      final GenericColumn metricVals = cachedMetricVals;
                      return new HistoricalLongColumnSelector()
                      {
                        @Override
                        public long get()
                        {
                          return metricVals.getLongSingleValueRow(cursorOffset.getOffset());
                        }

                        @Override
                        public long get(int rowNum)
                        {
                          return metricVals.getLongSingleValueRow(rowNum);
                        }

                        @Override
                        public String getLongColumnSelectorType()
                        {
                          return getClass().getName() + "["
                                 + "metricVals=" + metricVals.getGenericColumnType()
                                 + ", cursorOffset=" + cursorOffset.getOffsetType()
                                 + "]";
                        }
                      };
                    }


                    @Override
                    public ObjectColumnSelector makeObjectColumnSelector(String column)
                    {
                      Object cachedColumnVals = objectColumnCache.get(column);

                      if (cachedColumnVals == null) {
                        Column holder = index.getColumn(column);

                        if (holder != null) {
                          final ColumnCapabilities capabilities = holder.getCapabilities();

                          if (capabilities.isDictionaryEncoded()) {
                            cachedColumnVals = holder.getDictionaryEncoding();
                          } else if (capabilities.getType() == ValueType.COMPLEX) {
                            cachedColumnVals = holder.getComplexColumn();
                          } else {
                            cachedColumnVals = holder.getGenericColumn();
                          }
                        }

                        if (cachedColumnVals != null) {
                          closer.register((Closeable) cachedColumnVals);
                          objectColumnCache.put(column, cachedColumnVals);
                        }
                      }

                      if (cachedColumnVals == null) {
                        return null;
                      }

                      if (cachedColumnVals instanceof GenericColumn) {
                        final GenericColumn columnVals = (GenericColumn) cachedColumnVals;
                        final ValueType type = columnVals.getType();

                        if (columnVals.hasMultipleValues()) {
                          throw new UnsupportedOperationException(
                              "makeObjectColumnSelector does not support multi-value GenericColumns"
                          );
                        }
                        abstract class SingleValueObjectColumnSelector<T> extends ObjectColumnSelector<T>
                        {
                          @Override
                          public String getObjectColumnSelectorType()
                          {
                            return getClass().getName() + "["
                                   + "columnVals=" + columnVals.getGenericColumnType()
                                   + ", cursorOffset=" + cursorOffset.getOffsetType()
                                   + "]";
                          }
                        }
                        if (type == ValueType.FLOAT) {
                          return new SingleValueObjectColumnSelector<Float>()
                          {
                            @Override
                            public Class classOfObject()
                            {
                              return Float.TYPE;
                            }

                            @Override
                            public Float get()
                            {
                              return columnVals.getFloatSingleValueRow(cursorOffset.getOffset());
                            }
                          };
                        }
                        if (type == ValueType.LONG) {
                          return new SingleValueObjectColumnSelector<Long>()
                          {
                            @Override
                            public Class classOfObject()
                            {
                              return Long.TYPE;
                            }

                            @Override
                            public Long get()
                            {
                              return columnVals.getLongSingleValueRow(cursorOffset.getOffset());
                            }
                          };
                        }
                        if (type == ValueType.STRING) {
                          return new SingleValueObjectColumnSelector<String>()
                          {
                            @Override
                            public Class classOfObject()
                            {
                              return String.class;
                            }

                            @Override
                            public String get()
                            {
                              return columnVals.getStringSingleValueRow(cursorOffset.getOffset());
                            }
                          };
                        }
                      }

                      if (cachedColumnVals instanceof DictionaryEncodedColumn) {
                        final DictionaryEncodedColumn<String> columnVals = (DictionaryEncodedColumn) cachedColumnVals;
                        abstract class DictionaryEncodedObjectColumnSelector<T> extends ObjectColumnSelector<T>
                        {
                          @Override
                          public String getObjectColumnSelectorType()
                          {
                            return getClass().getName() + "["
                                   + "columnVals=" + columnVals.getDictionaryEncodedColumnType()
                                   + ", cursorOffset=" + cursorOffset.getOffsetType()
                                   + "]";
                          }
                        }
                        if (columnVals.hasMultipleValues()) {
                          return new DictionaryEncodedObjectColumnSelector<Object>()
                          {
                            @Override
                            public Class classOfObject()
                            {
                              return Object.class;
                            }

                            @Override
                            public Object get()
                            {
                              final IndexedInts multiValueRow = columnVals.getMultiValueRow(cursorOffset.getOffset());
                              if (multiValueRow.size() == 0) {
                                return null;
                              } else if (multiValueRow.size() == 1) {
                                return columnVals.lookupName(multiValueRow.get(0));
                              } else {
                                final String[] strings = new String[multiValueRow.size()];
                                for (int i = 0; i < multiValueRow.size(); i++) {
                                  strings[i] = columnVals.lookupName(multiValueRow.get(i));
                                }
                                return strings;
                              }
                            }
                          };
                        } else {
                          return new DictionaryEncodedObjectColumnSelector<String>()
                          {
                            @Override
                            public Class classOfObject()
                            {
                              return String.class;
                            }

                            @Override
                            public String get()
                            {
                              return columnVals.lookupName(columnVals.getSingleValueRow(cursorOffset.getOffset()));
                            }
                          };
                        }
                      }

                      final ComplexColumn columnVals = (ComplexColumn) cachedColumnVals;
                      return new ObjectColumnSelector()
                      {
                        @Override
                        public Class classOfObject()
                        {
                          return columnVals.getClazz();
                        }

                        @Override
                        public Object get()
                        {
                          return columnVals.getRowValue(cursorOffset.getOffset());
                        }

                        @Override
                        public String getObjectColumnSelectorType()
                        {
                          return getClass().getName() + "["
                                 + "columnVals=" + columnVals.getComplexColumnType()
                                 + ", cursorOffset=" + cursorOffset.getOffsetType()
                                 + "]";
                        }
                      };
                    }

                    @Override
                    public ColumnCapabilities getColumnCapabilities(String columnName)
                    {
                      return getColumnCapabilites(index, columnName);
                    }
                  }

                  if (postFilter == null) {
                    class NoPostFilterQueryableIndexBaseCursor extends QueryableIndexBaseCursor
                        implements HistoricalCursor
                    {
                      {
                        reset();
                      }

                      @Override
                      public DateTime getTime()
                      {
                        return myBucket;
                      }

                      @Override
                      public void advance()
                      {
                        BaseQuery.checkInterrupted();
                        cursorOffset.increment();
                      }

                      @Override
                      public void advanceWithoutInterruptedException()
                      {
                        cursorOffset.increment();
                      }

                      @Override
                      public void advanceTo(int offset)
                      {
                        int count = 0;
                        while (count < offset && !isDone()) {
                          advance();
                          count++;
                        }
                      }

                      @Override
                      public boolean isDone()
                      {
                        return !cursorOffset.withinBounds();
                      }

                      @Override
                      public boolean isDoneOrInterrupted()
                      {
                        return isDone() || Thread.currentThread().isInterrupted();
                      }

                      @Override
                      public void reset()
                      {
                        cursorOffset = resetOffset();
                        cursorOffsetHolder.set(cursorOffset);
                      }
                    }
                    return new NoPostFilterQueryableIndexBaseCursor();
                  } else {
                    return new QueryableIndexBaseCursor()
                    {
                      final CursorOffsetHolderValueMatcherFactory valueMatcherFactory = new CursorOffsetHolderValueMatcherFactory(
                          index,
                          this
                      );
                      final RowOffsetMatcherFactory rowOffsetMatcherFactory = new CursorOffsetHolderRowOffsetMatcherFactory(
                          cursorOffsetHolder,
                          descending
                      );

                      final ValueMatcher filterMatcher;
                      {
                        if (postFilter instanceof BooleanFilter) {
                          filterMatcher = ((BooleanFilter) postFilter).makeMatcher(
                              bitmapIndexSelector,
                              valueMatcherFactory,
                              rowOffsetMatcherFactory
                          );
                        } else {
                          if (postFilter.supportsBitmapIndex(bitmapIndexSelector)) {
                            filterMatcher = rowOffsetMatcherFactory.makeRowOffsetMatcher(
                                postFilter.getBitmapIndex(bitmapIndexSelector).getBitmap());
                          } else {
                            filterMatcher = postFilter.makeMatcher(valueMatcherFactory);
                          }
                        }
                      }

                      {
                        reset();
                      }

                      @Override
                      public DateTime getTime()
                      {
                        return myBucket;
                      }

                      @Override
                      public void advance()
                      {
                        BaseQuery.checkInterrupted();
                        cursorOffset.increment();

                        while (!isDone()) {
                          BaseQuery.checkInterrupted();
                          if (filterMatcher.matches()) {
                            return;
                          } else {
                            cursorOffset.increment();
                          }
                        }
                      }

                      @Override
                      public void advanceWithoutInterruptedException()
                      {
                        if (Thread.currentThread().isInterrupted()) {
                          return;
                        }
                        cursorOffset.increment();

                        while (!isDoneOrInterrupted()) {
                          if (filterMatcher.matches()) {
                            return;
                          } else {
                            cursorOffset.increment();
                          }
                        }
                      }

                      @Override
                      public void advanceTo(int offset)
                      {
                        int count = 0;
                        while (count < offset && !isDone()) {
                          advance();
                          count++;
                        }
                      }

                      @Override
                      public boolean isDone()
                      {
                        return !cursorOffset.withinBounds();
                      }

                      @Override
                      public boolean isDoneOrInterrupted()
                      {
                        return isDone() || Thread.currentThread().isInterrupted();
                      }

                      @Override
                      public void reset()
                      {
                        cursorOffset = resetOffset();
                        cursorOffsetHolder.set(cursorOffset);
                        if (!isDone()) {
                          if (filterMatcher.matches()) {
                            return;
                          } else {
                            advance();
                          }
                        }
                      }
                    };
                  }
                }

                private GenericColumn getCachedMetricVals(String columnName)
                {
                  GenericColumn cachedMetricVals = genericColumnCache.get(columnName);

                  if (cachedMetricVals == null) {
                    Column holder = index.getColumn(columnName);
                    if (holder != null && (holder.getCapabilities().getType() == ValueType.FLOAT
                                           || holder.getCapabilities().getType() == ValueType.LONG)) {
                      cachedMetricVals = holder.getGenericColumn();
                      closer.register(cachedMetricVals);
                      genericColumnCache.put(columnName, cachedMetricVals);
                    }
                  }
                  return cachedMetricVals;
                }
              }
          ),
          closer
      );
    }
  }

  public static class CursorOffsetHolder
  {
    Offset currOffset = null;

    public Offset get()
    {
      return currOffset;
    }

    public void set(Offset currOffset)
    {
      this.currOffset = currOffset;
    }
  }

  private static class CursorOffsetHolderValueMatcherFactory implements ValueMatcherFactory
  {
    private final ColumnSelector index;
    private final ColumnSelectorFactory cursor;

    public CursorOffsetHolderValueMatcherFactory(
        ColumnSelector index,
        ColumnSelectorFactory cursor
    )
    {
      this.index = index;
      this.cursor = cursor;
    }

    @Override
    public ValueMatcher makeValueMatcher(String dimension, final String value)
    {
      if (getTypeForDimension(dimension) == ValueType.LONG) {
        return Filters.getLongValueMatcher(
            cursor.makeLongColumnSelector(dimension),
            value
        );
      }

      final DimensionSelector selector = cursor.makeDimensionSelector(
          new DefaultDimensionSpec(dimension, dimension)
      );

      // if matching against null, rows with size 0 should also match
      final boolean matchNull = Strings.isNullOrEmpty(value);

      final int id = selector.lookupId(value);
      if (id < 0) {
        return new BooleanValueMatcher(false);
      } else {
        return new ValueMatcher()
        {
          @Override
          public boolean matches()
          {
            IndexedInts row = selector.getRow();
            if (row.size() == 0) {
              return matchNull;
            }
            for (int i = 0; i < row.size(); i++) {
              if (row.get(i) == id) {
                return true;
              }
            }
            return false;
          }
        };
      }
    }

    @Override
    public ValueMatcher makeValueMatcher(String dimension, final DruidPredicateFactory predicateFactory)
    {
      ValueType type = getTypeForDimension(dimension);
      switch (type) {
        case LONG:
          return makeLongValueMatcher(dimension, predicateFactory.makeLongPredicate());
        case STRING:
          return makeStringValueMatcher(dimension, predicateFactory.makeStringPredicate());
        default:
          return new BooleanValueMatcher(predicateFactory.makeStringPredicate().apply(null));
      }
    }

    private ValueMatcher makeStringValueMatcher(String dimension, final Predicate<String> predicate)
    {
      final DimensionSelector selector = cursor.makeDimensionSelector(
          new DefaultDimensionSpec(dimension, dimension)
      );

      return new ValueMatcher()
      {
        final boolean matchNull = predicate.apply(null);

        @Override
        public boolean matches()
        {
          IndexedInts row = selector.getRow();
          if (row.size() == 0) {
            return matchNull;
          }
          for (int i = 0; i < row.size(); i++) {
            if (predicate.apply(selector.lookupName(row.get(i)))) {
              return true;
            }
          }
          return false;
        }
      };
    }

    private ValueMatcher makeLongValueMatcher(String dimension, final DruidLongPredicate predicate)
    {
      return Filters.getLongPredicateMatcher(
          cursor.makeLongColumnSelector(dimension),
          predicate
      );
    }

    private ValueType getTypeForDimension(String dimension)
    {
      ColumnCapabilities capabilities = getColumnCapabilites(index, dimension);
      return capabilities == null ? ValueType.STRING : capabilities.getType();
    }
  }

  private static class CursorOffsetHolderRowOffsetMatcherFactory implements RowOffsetMatcherFactory
  {
    private final CursorOffsetHolder holder;
    private final boolean descending;

    public CursorOffsetHolderRowOffsetMatcherFactory(CursorOffsetHolder holder, boolean descending)
    {
      this.holder = holder;
      this.descending = descending;
    }

    // Use an iterator-based implementation, ImmutableBitmap.get(index) works differently for Concise and Roaring.
    // ImmutableConciseSet.get(index) is also inefficient, it performs a linear scan on each call
    @Override
    public ValueMatcher makeRowOffsetMatcher(final ImmutableBitmap rowBitmap) {
      final IntIterator iter = descending ?
                               BitmapOffset.getReverseBitmapOffsetIterator(rowBitmap) :
                               rowBitmap.iterator();

      if(!iter.hasNext()) {
        return new BooleanValueMatcher(false);
      }

      if (descending) {
        return new ValueMatcher()
        {
          int iterOffset = Integer.MAX_VALUE;

          @Override
          public boolean matches()
          {
            int currentOffset = holder.get().getOffset();
            while (iterOffset > currentOffset && iter.hasNext()) {
              iterOffset = iter.next();
            }

            return iterOffset == currentOffset;
          }
        };
      } else {
        return new ValueMatcher()
        {
          int iterOffset = -1;

          @Override
          public boolean matches()
          {
            int currentOffset = holder.get().getOffset();
            while (iterOffset < currentOffset && iter.hasNext()) {
              iterOffset = iter.next();
            }

            return iterOffset == currentOffset;
          }
        };
      }
    }
  }


  private abstract static class TimestampCheckingOffset extends Offset
  {
    protected final Offset baseOffset;
    protected final GenericColumn timestamps;
    protected final long timeLimit;

    public TimestampCheckingOffset(
        Offset baseOffset,
        GenericColumn timestamps,
        long timeLimit
    )
    {
      this.baseOffset = baseOffset;
      this.timestamps = timestamps;
      this.timeLimit = timeLimit;
    }

    @Override
    public int getOffset()
    {
      return baseOffset.getOffset();
    }

    @Override
    public boolean withinBounds()
    {
      if (!baseOffset.withinBounds()) {
        return false;
      }
      return timeInRange(timestamps.getLongSingleValueRow(baseOffset.getOffset()));
    }

    protected abstract boolean timeInRange(long current);

    @Override
    public void increment()
    {
      baseOffset.increment();
    }

    @Override
    public abstract Offset clone();

    @Override
    public String getOffsetType()
    {
      return getClass().getName() + "["
             + "baseOffset=" + baseOffset.getOffsetType()
             + ", timestamps=" + timestamps.getGenericColumnType()
             + "]";
    }
  }

  public static final class AscendingTimestampCheckingOffset extends TimestampCheckingOffset
  {
    public AscendingTimestampCheckingOffset(
        Offset baseOffset,
        GenericColumn timestamps,
        long timeLimit
    )
    {
      super(baseOffset, timestamps, timeLimit);
    }

    @Override
    protected final boolean timeInRange(long current)
    {
      return current < timeLimit;
    }

    @Override
    public String toString()
    {
      return (baseOffset.withinBounds() ? timestamps.getLongSingleValueRow(baseOffset.getOffset()) : "OOB") +
             "<" + timeLimit + "::" + baseOffset;
    }

    @Override
    public AscendingTimestampCheckingOffset clone()
    {
      return new AscendingTimestampCheckingOffset(baseOffset.clone(), timestamps, timeLimit);
    }
  }

  public static final class DescendingTimestampCheckingOffset extends TimestampCheckingOffset
  {
    public DescendingTimestampCheckingOffset(
        Offset baseOffset,
        GenericColumn timestamps,
        long timeLimit
    )
    {
      super(baseOffset, timestamps, timeLimit);
    }

    @Override
    protected final boolean timeInRange(long current)
    {
      return current >= timeLimit;
    }

    @Override
    public String toString()
    {
      return timeLimit + ">=" +
             (baseOffset.withinBounds() ? timestamps.getLongSingleValueRow(baseOffset.getOffset()) : "OOB") +
             "::" + baseOffset;
    }

    @Override
    public DescendingTimestampCheckingOffset clone()
    {
      return new DescendingTimestampCheckingOffset(baseOffset.clone(), timestamps, timeLimit);
    }
  }

  public static final class NoFilterOffset extends Offset
  {
    private final int rowCount;
    private final boolean descending;
    private int currentOffset;

    NoFilterOffset(int currentOffset, int rowCount, boolean descending)
    {
      this.currentOffset = currentOffset;
      this.rowCount = rowCount;
      this.descending = descending;
    }

    @Override
    public void increment()
    {
      currentOffset++;
    }

    @Override
    public boolean withinBounds()
    {
      return currentOffset < rowCount;
    }

    @Override
    public Offset clone()
    {
      return new NoFilterOffset(currentOffset, rowCount, descending);
    }

    @Override
    public int getOffset()
    {
      return descending ? rowCount - currentOffset - 1 : currentOffset;
    }

    @Override
    public String getOffsetType()
    {
      return getClass().getName() + "[descending=" + descending + "]";
    }

    @Override
    public String toString()
    {
      return currentOffset + "/" + rowCount + (descending ? "(DSC)" : "");
    }
  }

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }
}
