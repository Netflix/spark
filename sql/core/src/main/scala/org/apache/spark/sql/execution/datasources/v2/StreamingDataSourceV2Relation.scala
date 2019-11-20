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

import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.sources.v2.reader.streaming.{DataSourceReader, SupportsReportStatistics}

/**
 * A DSv2 relation for streaming.
 *
 * Note that, this plan has a mutable reader, so Spark won't apply operator push-down for this plan,
 * to avoid making the plan mutable.
 */
case class StreamingDataSourceV2Relation(
    output: Seq[AttributeReference],
    source: DataSourceV2,
    options: Map[String, String],
    reader: DataSourceReader)
  extends LeafNode with MultiInstanceRelation with StreamingDataSourceV2StringFormat {

  override def isStreaming: Boolean = true

  override def simpleString: String = "Streaming RelationV2 " + metadataString

  override def pushedFilters: Seq[Expression] = Nil

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: StreamingDataSourceV2Relation =>
      output == other.output && reader.getClass == other.reader.getClass && options == other.options
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(output, source, options).hashCode()
  }

  override def computeStats(): Statistics = reader match {
    case r: SupportsReportStatistics =>
      Statistics(sizeInBytes = r.estimateStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }
}
