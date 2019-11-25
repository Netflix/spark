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

package org.apache.spark.sql.connector

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{AnalysisSuite, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.{Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, TableCapabilityCheck}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TableCapabilityCheckSuite extends AnalysisSuite with SharedSparkSession {

  test("batch scan: check missing capabilities") {
    val e = intercept[AnalysisException] {
      TableCapabilityCheck.apply(DataSourceV2Relation.create(
        CapabilityTable(),
        CaseInsensitiveStringMap.empty))
    }
    assert(e.message.contains("does not support batch scan"))
  }

  test("AppendData: check missing capabilities") {
    val plan = AppendData.byName(
      DataSourceV2Relation.create(CapabilityTable(), CaseInsensitiveStringMap.empty), TestRelation)

    val exc = intercept[AnalysisException]{
      TableCapabilityCheck.apply(plan)
    }

    assert(exc.getMessage.contains("does not support append in batch mode"))
  }

  test("AppendData: check correct capabilities") {
    Seq(BATCH_WRITE, V1_BATCH_WRITE).foreach { write =>
      val plan = AppendData.byName(
        DataSourceV2Relation.create(CapabilityTable(write), CaseInsensitiveStringMap.empty),
        TestRelation)

      TableCapabilityCheck.apply(plan)
    }
  }

  test("Truncate: check missing capabilities") {
    Seq(CapabilityTable(),
      CapabilityTable(BATCH_WRITE),
      CapabilityTable(V1_BATCH_WRITE),
      CapabilityTable(TRUNCATE),
      CapabilityTable(OVERWRITE_BY_FILTER)).foreach { table =>

      val plan = OverwriteByExpression.byName(
        DataSourceV2Relation.create(table, CaseInsensitiveStringMap.empty), TestRelation,
        Literal(true))

      val exc = intercept[AnalysisException]{
        TableCapabilityCheck.apply(plan)
      }

      assert(exc.getMessage.contains("does not support truncate in batch mode"))
    }
  }

  test("Truncate: check correct capabilities") {
    Seq(CapabilityTable(BATCH_WRITE, TRUNCATE),
      CapabilityTable(V1_BATCH_WRITE, TRUNCATE),
      CapabilityTable(BATCH_WRITE, OVERWRITE_BY_FILTER),
      CapabilityTable(V1_BATCH_WRITE, OVERWRITE_BY_FILTER)).foreach { table =>

      val plan = OverwriteByExpression.byName(
        DataSourceV2Relation.create(table, CaseInsensitiveStringMap.empty), TestRelation,
        Literal(true))

      TableCapabilityCheck.apply(plan)
    }
  }

  test("OverwriteByExpression: check missing capabilities") {
    Seq(CapabilityTable(),
      CapabilityTable(V1_BATCH_WRITE),
      CapabilityTable(BATCH_WRITE),
      CapabilityTable(OVERWRITE_BY_FILTER)).foreach { table =>

      val plan = OverwriteByExpression.byName(
        DataSourceV2Relation.create(table, CaseInsensitiveStringMap.empty), TestRelation,
        EqualTo(AttributeReference("x", LongType)(), Literal(5)))

      val exc = intercept[AnalysisException]{
        TableCapabilityCheck.apply(plan)
      }

      assert(exc.getMessage.contains("does not support overwrite by filter in batch mode"))
    }
  }

  test("OverwriteByExpression: check correct capabilities") {
    Seq(BATCH_WRITE, V1_BATCH_WRITE).foreach { write =>
      val table = CapabilityTable(write, OVERWRITE_BY_FILTER)
      val plan = OverwriteByExpression.byName(
        DataSourceV2Relation.create(table, CaseInsensitiveStringMap.empty), TestRelation,
        EqualTo(AttributeReference("x", LongType)(), Literal(5)))

      TableCapabilityCheck.apply(plan)
    }
  }

  test("OverwritePartitionsDynamic: check missing capabilities") {
    Seq(CapabilityTable(),
      CapabilityTable(BATCH_WRITE),
      CapabilityTable(OVERWRITE_DYNAMIC)).foreach { table =>

      val plan = OverwritePartitionsDynamic.byName(
        DataSourceV2Relation.create(table, CaseInsensitiveStringMap.empty), TestRelation)

      val exc = intercept[AnalysisException] {
        TableCapabilityCheck.apply(plan)
      }

      assert(exc.getMessage.contains("does not support dynamic overwrite in batch mode"))
    }
  }

  test("OverwritePartitionsDynamic: check correct capabilities") {
    val table = CapabilityTable(BATCH_WRITE, OVERWRITE_DYNAMIC)
    val plan = OverwritePartitionsDynamic.byName(
      DataSourceV2Relation.create(table, CaseInsensitiveStringMap.empty), TestRelation)

    TableCapabilityCheck.apply(plan)
  }
}

private object TableCapabilityCheckSuite {
  val schema: StructType = new StructType().add("id", LongType).add("data", StringType)
}

private case object TestRelation extends LeafNode with NamedRelation {
  override def name: String = "source_relation"
  override def output: Seq[AttributeReference] = TableCapabilityCheckSuite.schema.toAttributes
}

private object TestTableProvider extends TableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    throw new UnsupportedOperationException
  }
}

private case class CapabilityTable(_capabilities: TableCapability*) extends Table {
  override def name(): String = "capability_test_table"
  override def schema(): StructType = TableCapabilityCheckSuite.schema
  override def capabilities(): util.Set[TableCapability] = _capabilities.toSet.asJava
}

