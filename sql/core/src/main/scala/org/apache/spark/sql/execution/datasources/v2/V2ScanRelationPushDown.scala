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

import org.apache.spark.sql.catalyst.expressions.{And, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.DataSourceStrategy

object V2ScanRelationPushDown extends Rule[LogicalPlan] {
  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case PhysicalOperation(project, filters, relation: DataSourceV2Relation) =>
      val scanBuilder = relation.table.asReadable.newScanBuilder(relation.options)

      val (withSubquery, withoutSubquery) = filters.partition(SubqueryExpression.hasSubquery)
      val normalizedFilters = DataSourceStrategy.normalizeFilters(
        withoutSubquery, relation.output)

      // `pushedFilters` will be pushed down and evaluated in the underlying data sources.
      // `postScanFilters` need to be evaluated after the scan.
      // `postScanFilters` and `pushedFilters` can overlap, e.g. the parquet row group filter.
      val (pushedFilters, postScanFiltersWithoutSubquery) = PushDownUtils.pushFilters(
        scanBuilder, normalizedFilters)
      val postScanFilters = postScanFiltersWithoutSubquery ++ withSubquery
      val (scan, output) = PushDownUtils.pruneColumns(
        scanBuilder, relation, project ++ postScanFilters)
      logInfo(
        s"""
           |Pushing operators to ${relation.name}
           |Pushed Filters: ${pushedFilters.mkString(", ")}
           |Post-Scan Filters: ${postScanFilters.mkString(",")}
           |Output: ${output.mkString(", ")}
         """.stripMargin)

      val scanRelation = DataSourceV2ScanRelation(relation.table, scan, output)

      val filterCondition = postScanFilters.reduceLeftOption(And)
      val withFilter = filterCondition.map(Filter(_, scanRelation)).getOrElse(scanRelation)

      val withProjection = if (withFilter.output != project) {
        Project(project, withFilter)
      } else {
        withFilter
      }

      withProjection
  }
}
