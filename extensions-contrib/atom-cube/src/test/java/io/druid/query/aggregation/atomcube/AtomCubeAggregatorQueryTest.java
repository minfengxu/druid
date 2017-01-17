package io.druid.query.aggregation.atomcube;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Pair;
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.*;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.*;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.timeseries.*;
import io.druid.query.topn.*;
import io.druid.segment.*;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class AtomCubeAggregatorQueryTest
{
  private static final String BITMAP_TYPE = "roaring";

  private static final String D_USER_NAME = "user_name";
  private static final String D_CITY_NAME = "city_name";

  private static final String M_USER_ID = "user_id";

  private static final DateTime TIME = new DateTime("2016-03-04T00:00:00.000Z");

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final IndexBuilder indexBuilder;
  private final Function<IndexBuilder, Pair<Segment, Closeable>> finisher;

  private Segment segment;
  private Closeable closeable;

  public AtomCubeAggregatorQueryTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<Segment, Closeable>> finisher
  )
  {
    this.indexBuilder = indexBuilder;
    this.finisher = finisher;
  }

  @Before
  public void setUp() throws Exception
  {
    AtomCubeDruidModule module = new AtomCubeDruidModule();
    module.configure(null);

    long timestamp = TIME.getMillis();

    indexBuilder
        .tmpDir(temporaryFolder.newFolder())
        .schema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(
                    new DimensionsSpec(
                        Lists.asList(D_USER_NAME, D_CITY_NAME, new String[0]),
                        null,
                        null
                    )
                )
                .withMetrics(
                    new AggregatorFactory[]{
                        new CountAggregatorFactory("count"),
                        new AtomCubeAggregatorFactory("atom_cube_user_id", M_USER_ID, BITMAP_TYPE)
                    }
                )
                .build()
        ).add(
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(D_USER_NAME, D_CITY_NAME, M_USER_ID),
            ImmutableMap.<String, Object>of(D_USER_NAME, "张三", D_CITY_NAME, "BeiJing", M_USER_ID, "281")
        ),
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(D_USER_NAME, D_CITY_NAME, M_USER_ID),
            ImmutableMap.<String, Object>of(D_USER_NAME, "李四", D_CITY_NAME, "BeiJing", M_USER_ID, "792")
        ),
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(D_USER_NAME, D_CITY_NAME, M_USER_ID),
            ImmutableMap.<String, Object>of(D_USER_NAME, "王五", D_CITY_NAME, "ShangHai", M_USER_ID, "863")
        )
    );

    final Pair<Segment, Closeable> pair = finisher.apply(indexBuilder);
    segment = pair.lhs;
    closeable = pair.rhs;
  }

  @After
  public void tearDown() throws Exception
  {
    closeable.close();
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder() throws IOException
  {
    return makeConstructors();
  }

  public static Collection<Object[]> makeConstructors()
  {
    // TODO(gianm): Some kind of helper to reduce code duplication with BaseFilterTest

    final List<Object[]> constructors = Lists.newArrayList();

    final Map<String, IndexMerger> indexMergers = ImmutableMap.<String, IndexMerger>of(
//      "IndexMerger", TestHelper.getTestIndexMerger(),
        "IndexMergerV9", TestHelper.getTestIndexMergerV9()
    );

    final Map<String, Function<IndexBuilder, Pair<Segment, Closeable>>> finishers = ImmutableMap.of(
        "incremental", new Function<IndexBuilder, Pair<Segment, Closeable>>()
        {
          @Override
          public Pair<Segment, Closeable> apply(IndexBuilder input)
          {
            final IncrementalIndex index = input.buildIncrementalIndex();
            return Pair.<Segment, Closeable>of(
                new IncrementalIndexSegment(index, "atom_dummy"),
                new Closeable()
                {
                  @Override
                  public void close() throws IOException
                  {
                    index.close();
                  }
                }
            );
          }
        },
        "mmapped", new Function<IndexBuilder, Pair<Segment, Closeable>>()
        {
          @Override
          public Pair<Segment, Closeable> apply(IndexBuilder input)
          {
            final QueryableIndex index = input.buildMMappedIndex();
            return Pair.<Segment, Closeable>of(
                new QueryableIndexSegment("atom_dummy", index),
                new Closeable()
                {
                  @Override
                  public void close() throws IOException
                  {
                    index.close();
                  }
                }
            );
          }
        },
        "mmappedMerged", new Function<IndexBuilder, Pair<Segment, Closeable>>()
        {
          @Override
          public Pair<Segment, Closeable> apply(IndexBuilder input)
          {
            final QueryableIndex index = input.buildMMappedMergedIndex();
            return Pair.<Segment, Closeable>of(
                new QueryableIndexSegment("atom_dummy", index),
                new Closeable()
                {
                  @Override
                  public void close() throws IOException
                  {
                    index.close();
                  }
                }
            );
          }
        }
    );

    for (Map.Entry<String, IndexMerger> indexMergerEntry : indexMergers.entrySet()) {
      for (Map.Entry<String, Function<IndexBuilder, Pair<Segment, Closeable>>> finisherEntry : finishers.entrySet()) {
        final String testName = String.format(
            "indexMerger[%s], finisher[%s]",
            indexMergerEntry.getKey(),
            finisherEntry.getKey()
        );
        final IndexBuilder indexBuilder = IndexBuilder.create()
                                                      .indexMerger(indexMergerEntry.getValue());

        constructors.add(new Object[]{testName, indexBuilder, finisherEntry.getValue()});
        break;//xmf added to avoid run mmapped and mmappedMerged testcases due to delete template file failed in windows
      }

    }

    return constructors;
  }

  @Test
  public void testTimeseries() throws Exception
  {
    QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.allGran)
                                  .intervals(QueryRunnerTestHelper.fullOnInterval)
                                  .aggregators(
                                      Lists.newArrayList(
//          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory("count", "count"),
//          new AtomCubeAggregatorFactory("UV", VISITOR_ID, BITMAP_TYPE),
//          new AtomCubeAggregatorFactory("UV2", VISITOR_AtomCube, BITMAP_TYPE),
//          new AtomCubeAggregatorFactory("UC", CLIENT_AtomCube, BITMAP_TYPE)
                                          new AtomCubeAggregatorFactory("user_id", M_USER_ID, BITMAP_TYPE)
                                      )
                                  )
//      .postAggregators(
//        ImmutableList.<PostAggregator>of(
//          new AtomCubeSizePostAggregator(
//            "UV-intersect",
//            new AtomCubeSetPostAggregator(
//              "dummy",
//              AtomCubeSetPostAggregator.Func.INTERSECT.toString(),
//              ImmutableList.<PostAggregator>of(
//                new FieldAccessPostAggregator("dummy2", "UV"),
//                new FieldAccessPostAggregator("dummy3", "UV2")
//              )
//            )
//          ),
//          new AtomCubeRawPostAggregator(
//            "UV-list",
//            AtomCubeRawPostAggregator.Format.LIST.toString(),
//            new FieldAccessPostAggregator("dummy4", "UV")
//          ),
//          new AtomCubeRawPostAggregator(
//            "UV-roaringBase64",
//            AtomCubeRawPostAggregator.Format.ROARINGBASE64.toString(),
//            new FieldAccessPostAggregator("dummy5", "UV")
//          )
//        )
//      )
                                  .build();

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        new FinalizeResultsQueryRunner(
            factory.createRunner(segment),
            factory.getToolchest()
        ).run(query, Maps.newHashMap()),
        Lists.<Result<TimeseriesResultValue>>newLinkedList()
    );

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            TIME,
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("UV", 2)
                    .put("UV2", 2)
                    .put("UC", 3)
                    .put("rows", 3L)
                    .put("count", 3L)
                    .put("UV-intersect", 2)
                    .put("UV-list", ImmutableList.of(0, 2))
                    .put("UV-roaringBase64", "OjAAAAEAAAAAAAEAEAAAAAAAAgA=")
                    .build()
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTopN() throws Exception
  {
    QueryRunnerFactory factory = new TopNQueryRunnerFactory(
        TestQueryRunners.getPool(),
        new TopNQueryQueryToolChest(
            new TopNQueryConfig(),
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .dimension(D_USER_NAME)
        .metric(M_USER_ID)
        .threshold(5)
        .aggregators(
            Lists.newArrayList(
                new LongSumAggregatorFactory("count", "count"),
                new AtomCubeAggregatorFactory("user_id", M_USER_ID, BITMAP_TYPE)
            )
        )
        .build();

    Iterable<Result<TopNResultValue>> results = Sequences.toList(
        new FinalizeResultsQueryRunner(
            factory.createRunner(segment),
            factory.getToolchest()
        ).run(query, Maps.newHashMap()),
        Lists.newArrayList()
    );

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            TIME,
            new TopNResultValue(
                ImmutableList.of(
                    ImmutableMap.<String, Object>of(
                        D_USER_NAME, "张三",
                        M_USER_ID, 1,
                        "count", 1L
                    ),
                    ImmutableMap.<String, Object>of(
                        D_USER_NAME, "李四",
                        M_USER_ID, 1,
                        "count", 1L
                    ),
                    ImmutableMap.<String, Object>of(
                        D_USER_NAME, "王五",
                        M_USER_ID, 1,
                        "count", 1L
                    )
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  //@Test
  public void testGroupBy() throws Exception
  {
    final StupidPool<ByteBuffer> pool = new StupidPool<>(
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocate(1024 * 1024);
          }
        }
    );

    final GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);

    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByQueryEngine engine = new GroupByQueryEngine(configSupplier, pool);

    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        configSupplier,
        new GroupByQueryQueryToolChest(
            configSupplier, new DefaultObjectMapper(), engine, TestQueryRunners.pool,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        TestQueryRunners.pool
    );

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setGranularity(QueryRunnerTestHelper.allGran)
                                     .setInterval(QueryRunnerTestHelper.fullOnInterval)
                                     .setDimensions(
                                         ImmutableList.<DimensionSpec>of(
                                             new DefaultDimensionSpec(
                                                 M_USER_ID,
                                                 M_USER_ID
                                             )
                                         )
                                     )
                                     .setLimitSpec(
                                         new DefaultLimitSpec(
                                             ImmutableList.of(
                                                 new OrderByColumnSpec(
                                                     M_USER_ID,
                                                     OrderByColumnSpec.Direction.DESCENDING//,
//              null
                                                 )
                                             ),
                                             Integer.MAX_VALUE
                                         )
                                     )
                                     .setAggregatorSpecs(
                                         Lists.newArrayList(
                                             new LongSumAggregatorFactory("count", "count"),
//          new AtomCubeAggregatorFactory("UV", VISITOR_ID, BITMAP_TYPE),
//          new AtomCubeAggregatorFactory("UV2", VISITOR_AtomCube, BITMAP_TYPE),
//          new AtomCubeAggregatorFactory("UC", CLIENT_AtomCube, BITMAP_TYPE)
                                             new AtomCubeAggregatorFactory("user_id", M_USER_ID, BITMAP_TYPE)
                                         )
                                     )
                                     .build();

    List<Row> results = Sequences.toList(
        new FinalizeResultsQueryRunner(
            factory.getToolchest().mergeResults(
                factory.mergeRunners(
                    MoreExecutors.sameThreadExecutor(),
                    ImmutableList.of(factory.createRunner(segment))
                )
            ),
            factory.getToolchest()
        ).run(query, Maps.newHashMap()),
        Lists.newArrayList()
    );

    List<Row> expectedResults = Arrays.<Row>asList(
        new MapBasedRow(
            new DateTime(0),
            ImmutableMap.<String, Object>of(
                M_USER_ID, "00000002",
                "UC", 2,
                "UV", 1,
                "UV2", 1,
                "count", 2L
            )
        ),
        new MapBasedRow(
            new DateTime(0),
            ImmutableMap.<String, Object>of(
                M_USER_ID, "00000000",
                "UC", 1,
                "UV", 1,
                "UV2", 1,
                "count", 1L
            )
        )
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
