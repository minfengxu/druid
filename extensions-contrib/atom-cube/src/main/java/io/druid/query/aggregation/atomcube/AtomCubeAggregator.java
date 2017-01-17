package io.druid.query.aggregation.atomcube;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;
import org.roaringbitmap.RoaringBitmap;

/**
 * Created by minfengxu on 2016/4/22.
 */
public class AtomCubeAggregator implements Aggregator {
  private final String name;
  private final ObjectColumnSelector selector;

  private RoaringBitmap bitmap;

  public AtomCubeAggregator(String name, ObjectColumnSelector selector) {
    this.name = name;
    this.selector = selector;
    reset();
  }

  @Override
  public void aggregate() {
    Object object = selector.get();
    if(object instanceof RoaringBitmap) {
      union((RoaringBitmap)object);
    } else if(object instanceof String) {
      Integer tmp = Integer.parseInt((String)object);
      bitmap.add(tmp);
    } else {
      System.err.println("unknown class for atomcubeaggregator.aggregate:"+object.getClass().getName());
    }
  }

  @Override
  public void reset() {
    if(bitmap == null) {
      bitmap = new RoaringBitmap();
    } else {
      bitmap.clear();
    }
  }

  @Override
  public Object get() {
    return bitmap;
  }

  @Override
  public float getFloat() {
    throw new UnsupportedOperationException("AtomCubeAggregator does not support getFloat()");
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void close() {

  }

  @Override
  public long getLong() {
    throw new UnsupportedOperationException("AtomCubeAggregator does not support getLong()");
  }

  private void union(RoaringBitmap _bitmap) {
    if(!_bitmap.isEmpty()) {
      for(int i : _bitmap.toArray()) {
        bitmap.add(i);
      }
    }
  }
}
