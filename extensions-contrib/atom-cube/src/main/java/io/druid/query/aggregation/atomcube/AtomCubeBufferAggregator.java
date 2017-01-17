package io.druid.query.aggregation.atomcube;

import com.google.common.collect.Maps;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by minfengxu on 2016/4/22.
 */
public class AtomCubeBufferAggregator implements BufferAggregator {
  private final Map<Integer, Aggregator> buffer = Maps.newHashMap();
  private final ColumnSelectorFactory selectorFactory;
  private final AtomCubeAggregatorFactory aggregatorFactory;

  public AtomCubeBufferAggregator(AtomCubeAggregatorFactory AggFactory, ColumnSelectorFactory selFactory) {
    this.aggregatorFactory = AggFactory;
    this.selectorFactory = selFactory;
  }

  @Override
  public void init(ByteBuffer buf, int position) {
    buffer.put(position, aggregatorFactory.factorize(selectorFactory));
  }

  @Override
  public void aggregate(ByteBuffer buf, int position) {
    buffer.get(position).aggregate();
  }

  @Override
  public Object get(ByteBuffer buf, int position) {
    return buffer.get(position).get();
  }

  @Override
  public float getFloat(ByteBuffer buf, int position) {
    throw new UnsupportedOperationException("AtomCubeAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position) {
    throw new UnsupportedOperationException("AtomCubeAggregator does not support getLong()");
  }

  @Override
  public void close() {

  }
}
