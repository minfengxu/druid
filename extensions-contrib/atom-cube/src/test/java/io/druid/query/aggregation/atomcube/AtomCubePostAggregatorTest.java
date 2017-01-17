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

import com.google.common.collect.Lists;
import com.metamx.emitter.EmittingLogger;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.collections.bitmap.RoaringBitmapFactory;
import io.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.query.aggregation.PostAggregator;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.IntConsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

;

public class AtomCubePostAggregatorTest
{
  private static final EmittingLogger log = new EmittingLogger(AtomCubePostAggregatorTest.class);

  public static final BitmapFactory bitmapFactory = new RoaringBitmapFactory();
  public ImmutableBitmap bitmapA;
  public ImmutableBitmap bitmapB;
  public ImmutableBitmap bitmapC;
  public ImmutableBitmap bitmapD;
  public ImmutableBitmap bitmapE;
  public ImmutableBitmap bitmapF;
  public ImmutableBitmap bitmapG;

  Map<String, Object> bitmaps = new HashMap<>();

  private enum TYPE
  {
    SET,
    SIZE,
    RAW
  }

  @Before
  public void setUp()
  {
    AtomCubeAggregatorFactory.BITMAP_FACTORY = bitmapFactory;
    MutableBitmap _bitmap = bitmapFactory.makeEmptyMutableBitmap();
    _bitmap.add(0);
    _bitmap.add(1);
    _bitmap.add(3);
    _bitmap.add(5);
    _bitmap.add(7);
    bitmapA = bitmapFactory.makeImmutableBitmap(_bitmap);

    _bitmap.clear();
    _bitmap.add(7);
    _bitmap.add(5);
    _bitmap.add(0);
    bitmapB = bitmapFactory.makeImmutableBitmap(_bitmap);

    _bitmap.clear();
    _bitmap.add(1);
    _bitmap.add(4);
    bitmapC = bitmapFactory.makeImmutableBitmap(_bitmap);

    _bitmap.clear();
    _bitmap.add(3);
    _bitmap.add(7);
    _bitmap.add(9);
    bitmapD = bitmapFactory.makeImmutableBitmap(_bitmap);

    _bitmap.clear();
    _bitmap.add(2141398);
    _bitmap.add(200131519);
    bitmapE = bitmapFactory.makeImmutableBitmap(_bitmap);

    _bitmap.clear();
    _bitmap.add(983);
    _bitmap.add(110488);
    _bitmap.add(584610);
    _bitmap.add(1148156);
    _bitmap.add(1618570);
    _bitmap.add(2054001);
    _bitmap.add(2141398);
    _bitmap.add(4439970);
    _bitmap.add(200131519);
    _bitmap.add(200555227);
    _bitmap.add(203558784);
    bitmapF = bitmapFactory.makeImmutableBitmap(_bitmap);

    _bitmap.clear();
    _bitmap.add(66185);
    _bitmap.add(94894);
    _bitmap.add(158015);
    _bitmap.add(1698436);
    _bitmap.add(3591822);
    _bitmap.add(5001496);
    _bitmap.add(200545076);
    _bitmap.add(201191980);
    _bitmap.add(202528541);
    _bitmap.add(252137459);
    bitmapG = bitmapFactory.makeImmutableBitmap(_bitmap);

    _bitmap.clear();
    _bitmap.add(1);
    _bitmap.add(2);
    bitmapE = bitmapFactory.makeImmutableBitmap(_bitmap);

    _bitmap.clear();
    _bitmap.add(1);
    _bitmap.add(5);
    _bitmap.add(6);
    _bitmap.add(7);
    _bitmap.add(8);
    _bitmap.add(2);
    bitmapF = bitmapFactory.makeImmutableBitmap(_bitmap);

    _bitmap.clear();
    _bitmap.add(3);
    _bitmap.add(4);
    _bitmap.add(11);
    _bitmap.add(7);
    _bitmap.add(9);
    _bitmap.add(10);
    bitmapG = bitmapFactory.makeImmutableBitmap(_bitmap);

    bitmaps.put("A", bitmapA);
    bitmaps.put("B", bitmapB);
    bitmaps.put("C", bitmapC);
    bitmaps.put("D", bitmapD);
    bitmaps.put("E", bitmapE);
    bitmaps.put("F", bitmapF);
    bitmaps.put("G", bitmapG);

  }

  @Test
  public void testSetPostAggregator_UNION()
  {
    log.debug("--- TestUnion ---");
    String expectResult = "[0,1,3,5,7]";
    ImmutableBitmap retBitmap = (ImmutableBitmap) genPostAggregator(
        TYPE.SET,
        AtomCubeSetPostAggregator.Func.UNION.name(),
        "A",
        "B"
    ).compute(bitmaps);
    String ret = printBitmap(retBitmap);
    assertEquals(expectResult, ret);
  }

  @Test
  public void testSetPostAggregator_INTERSECT()
  {
    log.debug("--- TestIntersect ---");
    String expectResult = "[3,7]";
    ImmutableBitmap retBitmap = (ImmutableBitmap) genPostAggregator(
        TYPE.SET,
        AtomCubeSetPostAggregator.Func.INTERSECT.name(),
        "A",
        "D"
    ).compute(bitmaps);
    String ret = printBitmap(retBitmap);
    assertEquals(expectResult, ret);
  }

  @Test
  public void testSetPostAggregator_NOT()
  {
    log.debug("--- TestNot ---");
    String expectResult = "[1,3]";
    ImmutableBitmap retBitmap = (ImmutableBitmap) genPostAggregator(
        TYPE.SET,
        AtomCubeSetPostAggregator.Func.NOT.name(),
        "A",
        "B"
    ).compute(bitmaps);
    String ret = printBitmap(retBitmap);
    assertEquals(expectResult, ret);

    log.debug("------------------");
    String expectResult1 = "[0,5]";
    ImmutableBitmap retBitmap1 = (ImmutableBitmap) genPostAggregator(
        TYPE.SET,
        AtomCubeSetPostAggregator.Func.NOT.name(),
        "B",
        "D"
    ).compute(bitmaps);
    String ret1 = printBitmap(retBitmap1);
    assertEquals(expectResult1, ret1);

    log.debug("------------------");
    String expectResult2 = "[5,6,7,8]";
    ImmutableBitmap retBitmap2 = (ImmutableBitmap) genPostAggregator(
        TYPE.SET,
        AtomCubeSetPostAggregator.Func.NOT.name(),
        "F",
        "E"
    ).compute(bitmaps);
    String ret2 = printBitmap(retBitmap2);
    assertEquals(expectResult2, ret2);

    log.debug("------------------");
    String expectResult3 = "[]";
    ImmutableBitmap retBitmap3 = (ImmutableBitmap) genPostAggregator(
        TYPE.SET,
        AtomCubeSetPostAggregator.Func.NOT.name(),
        "E",
        "F"
    ).compute(bitmaps);
    String ret3 = printBitmap(retBitmap3);
    assertEquals(expectResult3, ret3);

    log.debug("------------------");
    String expectResult4 = "[5,6,8]";
    ImmutableBitmap retBitmap4 = (ImmutableBitmap) genPostAggregator(
        TYPE.SET,
        AtomCubeSetPostAggregator.Func.NOT.name(),
        "F",
        "E",
        "G"
    ).compute(bitmaps);
    String ret4 = printBitmap(retBitmap4);
    assertEquals(expectResult4, ret4);
  }

  private String printBitmap(ImmutableBitmap bitmap)
  {
    final StringBuffer sb = new StringBuffer();
    sb.append('[');
    if (bitmap instanceof WrappedImmutableRoaringBitmap) {
      org.roaringbitmap.buffer.ImmutableRoaringBitmap _bitmap =
          ((WrappedImmutableRoaringBitmap) bitmap).getBitmap();
      _bitmap.forEach(new IntConsumer()
      {
        @Override
        public void accept(int value)
        {
          sb.append(value).append(',');
        }
      });
    }
//    IntIterator iter = bitmap.iterator();
//    while(iter.hasNext()) {
//      sb.append(iter.next()).append(',');
//    }
    if (sb.lastIndexOf(",") > 0) {
      sb.deleteCharAt(sb.lastIndexOf(","));
    }
    sb.append(']');
    log.debug(sb.toString());
    return sb.toString();
  }

  private PostAggregator genPostAggregator(TYPE type, String func, String... bitmapNames)
  {
    PostAggregator retValue = null;
    String name = "test";
    List<String> fields = Lists.newArrayList();
    for (int i = 0; i < bitmapNames.length; i++) {
      String s = bitmapNames[i];
      fields.add(s);
      printBitmap((ImmutableBitmap) bitmaps.get(s));
      if (i < bitmapNames.length - 1) {
        log.debug(func);
      } else {
        log.debug("=");
      }
    }
    if (type == TYPE.SET) {
      retValue = new AtomCubeSetPostAggregator(name, func, fields);
    }
    return retValue;
  }
}
