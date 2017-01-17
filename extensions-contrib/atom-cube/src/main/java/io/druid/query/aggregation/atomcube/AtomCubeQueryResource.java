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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapMaker;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.query.DruidMetrics;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.server.QueryStats;
import io.druid.server.RequestLogLine;
import io.druid.server.log.RequestLogger;
import org.joda.time.DateTime;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.UUID;

@Path("/druid/v2/atomcube")
public class AtomCubeQueryResource
{
  private static final EmittingLogger log = new EmittingLogger(AtomCubeQueryResource.class);

  private static final String APPLICATION_SMILE = "application/smile";
  private static final int RESPONSE_CTX_HEADER_LEN_LIMIT = 7 * 1024;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;

  private final QueryRunnerFactoryConglomerate conglomerate;


  @Inject
  public AtomCubeQueryResource(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      QueryRunnerFactoryConglomerate conglomerate
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.conglomerate = conglomerate;
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE, APPLICATION_SMILE})
  public Response doPost(
      InputStream in,
      @QueryParam("pretty") String pretty,
      @Context final HttpServletRequest req // used only to get request content-type and remote address
  ) throws IOException
  {
    final long start = System.currentTimeMillis();
    final String reqContentType = req.getContentType();
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(reqContentType)
                            || APPLICATION_SMILE.equals(reqContentType);
    ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;
    final String contentType = isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON;
    final ObjectWriter jsonWriter = pretty != null
                                    ? objectMapper.writerWithDefaultPrettyPrinter()
                                    : objectMapper.writer();

    AtomCubeQuery _query = objectMapper.readValue(in, AtomCubeQuery.class);
    final AtomCubeQuery atomQ = (AtomCubeQuery) _query.withId(UUID.randomUUID().toString());
    final Map<String, Object> responseContext1 = new MapMaker().makeMap();
    Sequence res = atomQ.run(this.conglomerate.findFactory(atomQ).createRunner(null), responseContext1);
    final Sequence results;
    if (res == null) {
      results = Sequences.empty();
    } else {
      results = res;
    }

    final Yielder yielder = results.toYielder(
        null,
        new YieldingAccumulator()
        {
          @Override
          public Object accumulate(Object accumulated, Object in)
          {
            yield();
            return in;
          }
        }
    );

    final Map<String, Object> responseContext = new MapMaker().makeMap();
    Response.ResponseBuilder builder = Response
        .ok(
            new StreamingOutput()
            {
              @Override
              public void write(OutputStream outputStream) throws IOException, WebApplicationException
              {
                CountingOutputStream os = new CountingOutputStream(outputStream);
                jsonWriter.writeValue(os, yielder);

                os.flush();
                os.close();

                final long queryTime = System.currentTimeMillis() - start;
                emitter.emit(
                    DruidMetrics.makeQueryTimeMetric(null, jsonMapper, atomQ, req.getRemoteAddr())
                                .setDimension("success", "true")
                                .build("query/time", queryTime)
                );
                emitter.emit(
                    DruidMetrics.makeQueryTimeMetric(null, jsonMapper, atomQ, req.getRemoteAddr())
                                .build("query/bytes", os.getCount())
                );

                requestLogger.log(
                    new RequestLogLine(
                        new DateTime(),
                        req.getRemoteAddr(),
                        atomQ,
                        new QueryStats(
                            ImmutableMap.<String, Object>of(
                                "query/time", queryTime,
                                "query/bytes", os.getCount(),
                                "success", true
                            )
                        )
                    )
                );
              }
            }, contentType
        ).header("X-Druid-Query-Id", atomQ.getId());

    String responseCtxString = jsonMapper.writeValueAsString(responseContext);

    return builder
        .header("X-Druid-Response-Context", responseCtxString)
        .build();
  }
}
