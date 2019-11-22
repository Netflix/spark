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

import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, Statistics => V2Statistics, SupportsReportStatistics}
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

/**
 * A logical plan representing a data source v2 table.
 *
 * @param table   The table that this relation represents.
 * @param options The options for this table operation. It's used to create fresh [[ScanBuilder]]
 *                and [[WriteBuilder]].
 */
case class DataSourceV2Relation(
    table: Table,
    output: Seq[AttributeReference],
    options: CaseInsensitiveStringMap)
  extends LeafNode with MultiInstanceRelation with NamedRelation {

  import DataSourceV2Implicits._

  override def name: String = table.name()

  override def skipSchemaResolution: Boolean = table.supports(TableCapability.ACCEPT_ANY_SCHEMA)

  override def simpleString: String = {
    s"RelationV2${output.mkString("[", ", ", "]")} $name"
  }

  override def computeStats(): Statistics = {
    if (Utils.isTesting) {
      // when testing, throw an exception if this computeStats method is called because stats should
      // not be accessed before pushing the projection and filters to create a scan. otherwise, the
      // stats are not accurate because they are based on a full table scan of all columns.
      throw new IllegalStateException(
        s"BUG: computeStats called before pushdown on DSv2 relation: $name")
    } else {
      // when not testing, return stats because bad stats are better than failing a query
      table.asReadable.newScanBuilder(options) match {
        case r: SupportsReportStatistics =>
          val statistics = r.estimateStatistics()
          DataSourceV2Relation.transformV2Stats(statistics, None, conf.defaultSizeInBytes)
        case _ =>
          Statistics(sizeInBytes = conf.defaultSizeInBytes)
      }
    }
  }

  override def newInstance(): DataSourceV2Relation = {
    copy(output = output.map(_.newInstance()))
  }
}

/**
 * A logical plan for a DSv2 table with a scan already created.
 *
 * This is used in the optimizer to push filters and projection down before conversion to physical
 * plan. This ensures that the stats that are used by the optimizer account for the filters and
 * projection that will be pushed down.
 *
 * @param table a DSv2 [[Table]]
 * @param scan a DSv2 [[Scan]]
 * @param output the output attributes of this relation
 */
case class DataSourceV2ScanRelation(
    table: Table,
    scan: Scan,
    output: Seq[AttributeReference]) extends LeafNode with NamedRelation {

  override def name: String = table.name()

  override def simpleString: String = {
    s"RelationV2${output.mkString("[", ", ", "]")} $name"
  }

  override def computeStats(): Statistics = {
    scan match {
      case r: SupportsReportStatistics =>
        val statistics = r.estimateStatistics()
        DataSourceV2Relation.transformV2Stats(statistics, None, conf.defaultSizeInBytes)
      case _ =>
        Statistics(sizeInBytes = conf.defaultSizeInBytes)
    }
  }
}

object DataSourceV2Relation {
  def create(table: Table, options: CaseInsensitiveStringMap): DataSourceV2Relation = {
    val output = table.schema().toAttributes
    DataSourceV2Relation(table, output, options)
  }

  def create(table: Table): DataSourceV2Relation = create(table, CaseInsensitiveStringMap.empty)

  /**
   * This is used to transform data source v2 statistics to logical.Statistics.
   */
  def transformV2Stats(
      v2Statistics: V2Statistics,
      defaultRowCount: Option[BigInt],
      defaultSizeInBytes: Long): Statistics = {
    val numRows: Option[BigInt] = if (v2Statistics.numRows().isPresent) {
      Some(v2Statistics.numRows().getAsLong)
    } else {
      defaultRowCount
    }
    Statistics(
      sizeInBytes = v2Statistics.sizeInBytes().orElse(defaultSizeInBytes),
      rowCount = numRows)
  }
}
