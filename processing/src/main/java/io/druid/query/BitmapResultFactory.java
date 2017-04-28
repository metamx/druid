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

package io.druid.query;

import io.druid.collections.bitmap.ImmutableBitmap;

public interface BitmapResultFactory<T>
{
  T wrapUnknown(ImmutableBitmap bitmap);

  T wrapDimensionValue(ImmutableBitmap bitmap);

  T wrapAllFalse(ImmutableBitmap allFalseBitmap);

  T wrapAllTrue(ImmutableBitmap allTrueBitmap);

  boolean isEmpty(T bitmapResult);

  T intersection(Iterable<T> bitmapResults);

  T union(Iterable<T> bitmapResults);

  T unionDimensionValueBitmaps(Iterable<ImmutableBitmap> dimensionValueBitmaps);

  T complement(T bitmapResult, int numRows);

  ImmutableBitmap toImmutableBitmap(T bitmapResult);
}
