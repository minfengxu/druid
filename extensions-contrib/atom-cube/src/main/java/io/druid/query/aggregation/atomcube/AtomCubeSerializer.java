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

package io.druid.query.aggregation.atomcube;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.io.BaseEncoding;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.segment.data.BitmapSerdeFactory;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

public class AtomCubeSerializer<T extends RoaringBitmap> extends JsonSerializer<T>
{

  private final BitmapSerdeFactory bitmapSerdeFactory;

  public AtomCubeSerializer(BitmapSerdeFactory bitmapSerdeFactory)
  {
    this.bitmapSerdeFactory = bitmapSerdeFactory;
  }

  @Override
  public void serialize(RoaringBitmap value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException, JsonProcessingException
  {
    WrappedImmutableRoaringBitmap _bitmap = new WrappedImmutableRoaringBitmap(value.toMutableRoaringBitmap());
    jgen.writeString(serialize(_bitmap, bitmapSerdeFactory));
//    jgen.writeBinary(bitmapSerdeFactory.getObjectStrategy().toBytes(value));
  }

  static String serialize(ImmutableBitmap bitmap, BitmapSerdeFactory bitmapSerdeFactory)
  {
    return BaseEncoding.base64().encode(bitmapSerdeFactory.getObjectStrategy().toBytes(bitmap));
  }
}
