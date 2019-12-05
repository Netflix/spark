/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Repartition}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.streaming.continuous.{ContinuousCoalesceExec, WriteToContinuousDataSource, WriteToContinuousDataSourceExec}
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader

object StreamingDataSourceV2Strategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case r: StreamingDataSourceV2Relation =>
      // ensure there is a projection, which will produce unsafe rows required by some operators
      ProjectExec(r.output,
        StreamingDataSourceV2ScanExec(r.output, r.source, r.options, r.pushedFilters, r.reader)
      ) :: Nil

    case WriteToMicroBatchDataSourceV2(writer, query) =>
      WriteToMicroBatchDataSourceV2Exec(writer, planLater(query)) :: Nil

    case WriteToContinuousDataSource(writer, query) =>
      WriteToContinuousDataSourceExec(writer, planLater(query)) :: Nil

    case Repartition(1, false, child) =>
      val isContinuous = child.collectFirst {
        case StreamingDataSourceV2Relation(_, _, _, r: ContinuousReader) => r
      }.isDefined

      if (isContinuous) {
        ContinuousCoalesceExec(1, planLater(child)) :: Nil
      } else {
        Nil
      }

    case _ => Nil
  }
}
