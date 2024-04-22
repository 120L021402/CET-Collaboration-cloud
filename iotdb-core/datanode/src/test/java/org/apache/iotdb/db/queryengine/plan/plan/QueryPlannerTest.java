/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.plan;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.queryengine.common.DataNodeEndPoints;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.FakePartitionFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.analyze.FakeSchemaFetcherImpl;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.execution.QueryExecution;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeUtil;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.time.ZoneId;

public class QueryPlannerTest {

  private static IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      syncInternalServiceClientManager;

  private static IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
      asyncInternalServiceClientManager;
  private static final Coordinator COORDINATOR = Coordinator.getInstance();
  private final IPartitionFetcher partitionFetcher = ClusterPartitionFetcher.getInstance();
  private final ISchemaFetcher schemaFetcher = ClusterSchemaFetcher.getInstance();


  @BeforeClass
  public static void setUp() {
    syncInternalServiceClientManager =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    asyncInternalServiceClientManager =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncDataNodeInternalServiceClientPoolFactory());
  }

  @AfterClass
  public static void destroy() {
    syncInternalServiceClientManager.close();
  }

  @Ignore
  @Test
  public void testSqlToDistributedPlan() {

//    String querySql = "SELECT * FROM root.ln.wf02.wt02";
        String querySql = "SELECT * FROM root.sg LIMIT 10";
//    String querySql = "SELECT d1.*, d333.s1 FROM root.sg LIMIT 10";

    Statement stmt = StatementGenerator.createStatement(querySql, ZoneId.systemDefault());

//    MPPQueryContext mppQueryContext= new MPPQueryContext(
//            querySql,
//            new QueryId("query1"),
//            new SessionInfo(1L, "fakeUsername", "fakeZoneId"),
//            new TEndPoint(),
//            new TEndPoint());
//    MPPQueryContext mppQueryContext= new MPPQueryContext(
//            querySql,
//            new QueryId("query1"),
//            new SessionInfo(1L, "fakeUsername", "fakeZoneId"),
//            DataNodeEndPoints.LOCAL_HOST_DATA_BLOCK_ENDPOINT,
//            DataNodeEndPoints.LOCAL_HOST_INTERNAL_ENDPOINT);
//    mppQueryContext.setTimeOut(1000000000);
//    mppQueryContext.setStartTime(System.currentTimeMillis());

//    QueryExecution queryExecution =
//        new QueryExecution(
//            stmt,
//            mppQueryContext,
//            IoTDBThreadPoolFactory.newSingleThreadExecutor("test_query"),
//            IoTDBThreadPoolFactory.newSingleThreadExecutor("test_write_operation"),
//            IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("test_query_scheduled"),
//            new FakePartitionFetcherImpl(),
//            new FakeSchemaFetcherImpl(),
//            syncInternalServiceClientManager,
//            asyncInternalServiceClientManager);
    ExecutionResult result =
            COORDINATOR.execute(
                    stmt,
                    10086,
                    new SessionInfo(1L, "fakeUsername", "fakeZoneId"),
                    querySql,
                    partitionFetcher,
                    schemaFetcher,
                    1000000);
//    queryExecution.doLogicalPlan();
//    System.out.printf("SQL: %s%n%n", querySql);
//    System.out.println("===== Step 1: Logical Plan =====");
//    System.out.println(PlanNodeUtil.nodeToString(queryExecution.getLogicalPlan().getRootNode()));
//
//    queryExecution.doDistributedPlan();
//    DistributedQueryPlan distributedQueryPlan = queryExecution.getDistributedPlan();
//
//    System.out.println("===== Step 4: Split Fragment Instance =====");
//    distributedQueryPlan.getInstances().forEach(System.out::println);
//    queryExecution.start();
  }
}
