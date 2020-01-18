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

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NamespaceAlreadyExistsException, NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.sources.SimpleScanSource
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DataSourceV2SQLSuite
  extends InsertIntoTests(supportsDynamicOverwrite = true, includeSQLOnlyTests = true)
  with AlterTableTests {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private val v2Source = classOf[FakeV2Provider].getName
  override protected val v2Format = v2Source
  override protected val catalogAndNamespace = "testcat.ns1.ns2."

  private def catalog(name: String): CatalogPlugin = {
    spark.sessionState.catalogManager.catalog(name)
  }

  protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      sql(s"INSERT $overwrite TABLE $tableName SELECT * FROM $tmpView")
    }
  }

  override def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  override def getTableMetadata(tableName: String): Table = {
    val nameParts = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val v2Catalog = catalog(nameParts.head).asTableCatalog
    val namespace = nameParts.drop(1).init.toArray
    v2Catalog.loadTable(Identifier.of(namespace, nameParts.last))
  }

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    spark.conf.set(
        "spark.sql.catalog.testcat_atomic", classOf[StagingInMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.testcat2", classOf[InMemoryTableCatalog].getName)
    spark.conf.set(
      V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[InMemoryTableSessionCatalog].getName)

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")
  }

  after {
    spark.sessionState.catalog.reset()
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
  }

  test("CreateTable: use v2 plan because catalog is set") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)
  }

  test("DescribeTable using v2 catalog") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string)" +
      " USING foo" +
      " PARTITIONED BY (id)")
    val descriptionDf = spark.sql("DESCRIBE TABLE testcat.table_name")
    assert(descriptionDf.schema.map(field => (field.name, field.dataType)) ===
      Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
    val description = descriptionDf.collect()
    assert(description === Seq(
      Row("id", "bigint", ""),
      Row("data", "string", "")))
  }

  test("DescribeTable with v2 catalog when table does not exist.") {
    intercept[AnalysisException] {
      spark.sql("DESCRIBE TABLE testcat.table_name")
    }
  }

  test("DescribeTable extended using v2 catalog") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string)" +
      " USING foo" +
      " PARTITIONED BY (id)" +
      " TBLPROPERTIES ('bar'='baz')")
    val descriptionDf = spark.sql("DESCRIBE TABLE EXTENDED testcat.table_name")
    assert(descriptionDf.schema.map(field => (field.name, field.dataType))
      === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
    assert(descriptionDf.collect()
      .map(_.toSeq)
      .map(_.toArray.map(_.toString.trim)) === Array(
      Array("id", "bigint", ""),
      Array("data", "string", ""),
      Array("", "", ""),
      Array("Partitioning", "", ""),
      Array("--------------", "", ""),
      Array("Part 0", "id", ""),
      Array("", "", ""),
      Array("Table Property", "Value", ""),
      Array("----------------", "-------", ""),
      Array("bar", "baz", ""),
      Array("provider", "foo", "")))

  }

  test("CreateTable: use v2 plan and session catalog when provider is v2") {
    spark.sql(s"CREATE TABLE table_name (id bigint, data string) USING $v2Source")

    val testCatalog = catalog(SESSION_CATALOG_NAME).asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "default.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> v2Source).asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)
  }

  test("CreateTable: fail if table exists") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")

    val testCatalog = catalog("testcat").asTableCatalog

    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    // run a second create query that should fail
    val exc = intercept[TableAlreadyExistsException] {
      spark.sql("CREATE TABLE testcat.table_name (id bigint, data string, id2 bigint) USING bar")
    }

    assert(exc.getMessage.contains("table_name"))

    // table should not have changed
    val table2 = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table2.name == "testcat.table_name")
    assert(table2.partitioning.isEmpty)
    assert(table2.properties == Map("provider" -> "foo").asJava)
    assert(table2.schema == new StructType().add("id", LongType).add("data", StringType))

    // check that the table is still empty
    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)
  }

  test("CreateTable: if not exists") {
    spark.sql(
      "CREATE TABLE IF NOT EXISTS testcat.table_name (id bigint, data string) USING foo")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    spark.sql("CREATE TABLE IF NOT EXISTS testcat.table_name (id bigint, data string) USING bar")

    // table should not have changed
    val table2 = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table2.name == "testcat.table_name")
    assert(table2.partitioning.isEmpty)
    assert(table2.properties == Map("provider" -> "foo").asJava)
    assert(table2.schema == new StructType().add("id", LongType).add("data", StringType))

    // check that the table is still empty
    val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), Seq.empty)
  }

  test("CreateTable: use default catalog for v2 sources when default catalog is set") {
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "testcat")
    spark.sql(s"CREATE TABLE table_name (id bigint, data string) USING foo")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    // check that the table is empty
    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)
  }

  test("CreateTableAsSelect: use v2 plan because catalog is set") {
    val basicCatalog = catalog("testcat").asTableCatalog
    val atomicCatalog = catalog("testcat_atomic").asTableCatalog
    val basicIdentifier = "testcat.table_name"
    val atomicIdentifier = "testcat_atomic.table_name"

    Seq((basicCatalog, basicIdentifier), (atomicCatalog, atomicIdentifier)).foreach {
      case (catalog, identifier) =>
        spark.sql(s"CREATE TABLE $identifier USING foo AS SELECT id, data FROM source")

        val table = catalog.loadTable(Identifier.of(Array(), "table_name"))

        assert(table.name == identifier)
        assert(table.partitioning.isEmpty)
        assert(table.properties == Map("provider" -> "foo").asJava)
        assert(table.schema == new StructType()
          .add("id", LongType)
          .add("data", StringType))

        val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
        checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))
    }
  }

  test("ReplaceTableAsSelect: basic v2 implementation.") {
    val basicCatalog = catalog("testcat").asTableCatalog
    val atomicCatalog = catalog("testcat_atomic").asTableCatalog
    val basicIdentifier = "testcat.table_name"
    val atomicIdentifier = "testcat_atomic.table_name"

    Seq((basicCatalog, basicIdentifier), (atomicCatalog, atomicIdentifier)).foreach {
      case (catalog, identifier) =>
        spark.sql(s"CREATE TABLE $identifier USING foo AS SELECT id, data FROM source")
        val originalTable = catalog.loadTable(Identifier.of(Array(), "table_name"))

        spark.sql(s"REPLACE TABLE $identifier USING foo AS SELECT id FROM source")
        val replacedTable = catalog.loadTable(Identifier.of(Array(), "table_name"))

        assert(replacedTable != originalTable, "Table should have been replaced.")
        assert(replacedTable.name == identifier)
        assert(replacedTable.partitioning.isEmpty)
        assert(replacedTable.properties == Map("provider" -> "foo").asJava)
        assert(replacedTable.schema == new StructType().add("id", LongType))

        val rdd = spark.sparkContext.parallelize(replacedTable.asInstanceOf[InMemoryTable].rows)
        checkAnswer(
          spark.internalCreateDataFrame(rdd, replacedTable.schema),
          spark.table("source").select("id"))
    }
  }

  test("ReplaceTableAsSelect: Non-atomic catalog drops the table if the write fails.") {
    spark.sql("CREATE TABLE testcat.table_name USING foo AS SELECT id, data FROM source")
    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.asInstanceOf[InMemoryTable].rows.nonEmpty)

    intercept[Exception] {
      spark.sql("REPLACE TABLE testcat.table_name" +
        s" USING foo OPTIONS (`${InMemoryTable.SIMULATE_FAILED_WRITE_OPTION}`=true)" +
        s" AS SELECT id FROM source")
    }

    assert(!testCatalog.tableExists(Identifier.of(Array(), "table_name")),
        "Table should have been dropped as a result of the replace.")
  }

  test("ReplaceTableAsSelect: Non-atomic catalog drops the table permanently if the" +
    " subsequent table creation fails.") {
    spark.sql("CREATE TABLE testcat.table_name USING foo AS SELECT id, data FROM source")
    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.asInstanceOf[InMemoryTable].rows.nonEmpty)

    intercept[Exception] {
      spark.sql("REPLACE TABLE testcat.table_name" +
        s" USING foo" +
        s" TBLPROPERTIES (`${InMemoryTableCatalog.SIMULATE_FAILED_CREATE_PROPERTY}`=true)" +
        s" AS SELECT id FROM source")
    }

    assert(!testCatalog.tableExists(Identifier.of(Array(), "table_name")),
      "Table should have been dropped and failed to be created.")
  }

  test("ReplaceTableAsSelect: Atomic catalog does not drop the table when replace fails.") {
    spark.sql("CREATE TABLE testcat_atomic.table_name USING foo AS SELECT id, data FROM source")
    val testCatalog = catalog("testcat_atomic").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    intercept[Exception] {
      spark.sql("REPLACE TABLE testcat_atomic.table_name" +
        s" USING foo OPTIONS (`${InMemoryTable.SIMULATE_FAILED_WRITE_OPTION}=true)" +
        s" AS SELECT id FROM source")
    }

    var maybeReplacedTable = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(maybeReplacedTable === table, "Table should not have changed.")

    intercept[Exception] {
      spark.sql("REPLACE TABLE testcat_atomic.table_name" +
        s" USING foo" +
        s" TBLPROPERTIES (`${InMemoryTableCatalog.SIMULATE_FAILED_CREATE_PROPERTY}`=true)" +
        s" AS SELECT id FROM source")
    }

    maybeReplacedTable = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(maybeReplacedTable === table, "Table should not have changed.")
  }

  test("ReplaceTable: Erases the table contents and changes the metadata.") {
    spark.sql(s"CREATE TABLE testcat.table_name USING $v2Source AS SELECT id, data FROM source")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.asInstanceOf[InMemoryTable].rows.nonEmpty)

    spark.sql("REPLACE TABLE testcat.table_name (id bigint) USING foo")
    val replaced = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(replaced.asInstanceOf[InMemoryTable].rows.isEmpty,
        "Replaced table should have no rows after committing.")
    assert(replaced.schema().fields.length === 1,
        "Replaced table should have new schema.")
    assert(replaced.schema().fields(0).name === "id",
      "Replaced table should have new schema.")
  }

  test("ReplaceTableAsSelect: CREATE OR REPLACE new table has same behavior as CTAS.") {
    Seq("testcat", "testcat_atomic").foreach { catalogName =>
      spark.sql(
        s"""
           |CREATE TABLE $catalogName.created USING $v2Source
           |AS SELECT id, data FROM source
         """.stripMargin)
      spark.sql(
        s"""
           |CREATE OR REPLACE TABLE $catalogName.replaced USING $v2Source
           |AS SELECT id, data FROM source
         """.stripMargin)

      val testCatalog = catalog(catalogName).asTableCatalog
      val createdTable = testCatalog.loadTable(Identifier.of(Array(), "created"))
      val replacedTable = testCatalog.loadTable(Identifier.of(Array(), "replaced"))

      assert(createdTable.asInstanceOf[InMemoryTable].rows ===
        replacedTable.asInstanceOf[InMemoryTable].rows)
      assert(createdTable.schema === replacedTable.schema)
    }
  }

  test("ReplaceTableAsSelect: REPLACE TABLE throws exception if table does not exist.") {
    Seq("testcat", "testcat_atomic").foreach { catalog =>
      spark.sql(s"CREATE TABLE $catalog.created USING $v2Source AS SELECT id, data FROM source")
      intercept[CannotReplaceMissingTableException] {
        spark.sql(s"REPLACE TABLE $catalog.replaced USING $v2Source AS SELECT id, data FROM source")
      }
    }
  }

  test("ReplaceTableAsSelect: REPLACE TABLE throws exception if table is dropped before commit.") {
    import InMemoryTableCatalog._
    spark.sql(s"CREATE TABLE testcat_atomic.created USING $v2Source AS SELECT id, data FROM source")
    intercept[CannotReplaceMissingTableException] {
      spark.sql(s"REPLACE TABLE testcat_atomic.replaced" +
        s" USING $v2Source" +
        s" TBLPROPERTIES (`$SIMULATE_DROP_BEFORE_REPLACE_PROPERTY`=true)" +
        s" AS SELECT id, data FROM source")
    }
  }

  test("CreateTableAsSelect: use v2 plan and session catalog when provider is v2") {
    spark.sql(s"CREATE TABLE table_name USING $v2Source AS SELECT id, data FROM source")

    val testCatalog = catalog(SESSION_CATALOG_NAME).asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "default.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> v2Source).asJava)
    assert(table.schema == new StructType()
        .add("id", LongType)
        .add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: fail if table exists") {
    spark.sql("CREATE TABLE testcat.table_name USING foo AS SELECT id, data FROM source")

    val testCatalog = catalog("testcat").asTableCatalog

    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType()
        .add("id", LongType)
        .add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))

    // run a second CTAS query that should fail
    val exc = intercept[TableAlreadyExistsException] {
      spark.sql(
        "CREATE TABLE testcat.table_name USING bar AS SELECT id, data, id as id2 FROM source2")
    }

    assert(exc.getMessage.contains("table_name"))

    // table should not have changed
    val table2 = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table2.name == "testcat.table_name")
    assert(table2.partitioning.isEmpty)
    assert(table2.properties == Map("provider" -> "foo").asJava)
    assert(table2.schema == new StructType()
        .add("id", LongType)
        .add("data", StringType))

    val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: if not exists") {
    spark.sql(
      "CREATE TABLE IF NOT EXISTS testcat.table_name USING foo AS SELECT id, data FROM source")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType()
        .add("id", LongType)
        .add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))

    spark.sql(
      "CREATE TABLE IF NOT EXISTS testcat.table_name USING foo AS SELECT id, data FROM source2")

    // check that the table contains data from just the first CTAS
    val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: use default catalog for v2 sources when default catalog is set") {
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "testcat")

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")

    // setting the default catalog breaks the reference to source because the default catalog is
    // used and AsTableIdentifier no longer matches
    spark.sql(s"CREATE TABLE table_name USING foo AS SELECT id, data FROM source")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType()
        .add("id", LongType)
        .add("data", StringType))

    val rdd = sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: v2 session catalog can load v1 source table") {
    // unset this config to use the default v2 session catalog.
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")

    sql(s"CREATE TABLE table_name USING parquet AS SELECT id, data FROM source")

    checkAnswer(sql(s"TABLE default.table_name"), spark.table("source"))
    // The fact that the following line doesn't throw an exception means, the session catalog
    // can load the table.
    val t = catalog(SESSION_CATALOG_NAME).asTableCatalog
      .loadTable(Identifier.of(Array.empty, "table_name"))
    assert(t.isInstanceOf[V1Table], "V1 table wasn't returned as an unresolved table")
  }

  test("CreateTableAsSelect: nullable schema") {
    val basicCatalog = catalog("testcat").asTableCatalog
    val atomicCatalog = catalog("testcat_atomic").asTableCatalog
    val basicIdentifier = "testcat.table_name"
    val atomicIdentifier = "testcat_atomic.table_name"

    Seq((basicCatalog, basicIdentifier), (atomicCatalog, atomicIdentifier)).foreach {
      case (catalog, identifier) =>
        spark.sql(s"CREATE TABLE $identifier USING foo AS SELECT 1 i")

        val table = catalog.loadTable(Identifier.of(Array(), "table_name"))

        assert(table.name == identifier)
        assert(table.partitioning.isEmpty)
        assert(table.properties == Map("provider" -> "foo").asJava)
        assert(table.schema == new StructType().add("i", "int"))

        val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
        checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Row(1))

        sql(s"INSERT INTO $identifier SELECT CAST(null AS INT)")
        val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
        checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), Seq(Row(1), Row(null)))
    }
  }

  test("DropTable: basic") {
    val tableName = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    sql(s"CREATE TABLE $tableName USING foo AS SELECT id, data FROM source")
    assert(catalog("testcat").asTableCatalog.tableExists(ident) === true)
    sql(s"DROP TABLE $tableName")
    assert(catalog("testcat").asTableCatalog.tableExists(ident) === false)
  }

  test("DropTable: if exists") {
    intercept[NoSuchTableException] {
      sql(s"DROP TABLE testcat.db.notbl")
    }
    sql(s"DROP TABLE IF EXISTS testcat.db.notbl")
  }

  test("Relation: basic") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
      checkAnswer(sql(s"TABLE $t1"), spark.table("source"))
      checkAnswer(sql(s"SELECT * FROM $t1"), spark.table("source"))
    }
  }

  test("Relation: SparkSession.table()") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
      checkAnswer(spark.table(s"$t1"), spark.table("source"))
    }
  }

  test("Relation: CTE") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
      checkAnswer(
        sql(s"""
          |WITH cte AS (SELECT * FROM $t1)
          |SELECT * FROM cte
        """.stripMargin),
        spark.table("source"))
    }
  }

  test("Relation: view text") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      withView("view1") { v1: String =>
        sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
        sql(s"CREATE VIEW $v1 AS SELECT * from $t1")
        checkAnswer(sql(s"TABLE $v1"), spark.table("source"))
      }
    }
  }

  test("Relation: join tables in 2 catalogs") {
    val t1 = "testcat.ns1.ns2.tbl"
    val t2 = "testcat2.v2tbl"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
      sql(s"CREATE TABLE $t2 USING foo AS SELECT id, data FROM source2")
      val df1 = spark.table("source")
      val df2 = spark.table("source2")
      val df_joined = df1.join(df2).where(df1("id") + 1 === df2("id"))
      checkAnswer(
        sql(s"""
          |SELECT *
          |FROM $t1 t1, $t2 t2
          |WHERE t1.id + 1 = t2.id
        """.stripMargin),
        df_joined)
    }
  }

  test("InsertInto: append - across catalog") {
    val t1 = "testcat.ns1.ns2.tbl"
    val t2 = "testcat2.db.tbl"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT * FROM source")
      sql(s"CREATE TABLE $t2 (id bigint, data string) USING foo")
      sql(s"INSERT INTO $t2 SELECT * FROM $t1")
      checkAnswer(spark.table(t2), spark.table("source"))
    }
  }

  test("ShowTables: using v2 catalog") {
    spark.sql("CREATE TABLE testcat.db.table_name (id bigint, data string) USING foo")
    spark.sql("CREATE TABLE testcat.n1.n2.db.table_name (id bigint, data string) USING foo")

    runShowTablesSql("SHOW TABLES FROM testcat.db", Seq(Row("db", "table_name")))

    runShowTablesSql(
      "SHOW TABLES FROM testcat.n1.n2.db",
      Seq(Row("n1.n2.db", "table_name")))
  }

  test("ShowTables: using v2 catalog with a pattern") {
    spark.sql("CREATE TABLE testcat.db.table (id bigint, data string) USING foo")
    spark.sql("CREATE TABLE testcat.db.table_name_1 (id bigint, data string) USING foo")
    spark.sql("CREATE TABLE testcat.db.table_name_2 (id bigint, data string) USING foo")
    spark.sql("CREATE TABLE testcat.db2.table_name_2 (id bigint, data string) USING foo")

    runShowTablesSql(
      "SHOW TABLES FROM testcat.db",
      Seq(
        Row("db", "table"),
        Row("db", "table_name_1"),
        Row("db", "table_name_2")))

    runShowTablesSql(
      "SHOW TABLES FROM testcat.db LIKE '*name*'",
      Seq(Row("db", "table_name_1"), Row("db", "table_name_2")))

    runShowTablesSql(
      "SHOW TABLES FROM testcat.db LIKE '*2'",
      Seq(Row("db", "table_name_2")))
  }

  test("ShowTables: using v2 catalog, namespace doesn't exist") {
    runShowTablesSql("SHOW TABLES FROM testcat.unknown", Seq())
  }

  test("ShowTables: using v1 catalog") {
    runShowTablesSql(
      "SHOW TABLES FROM default",
      Seq(Row("", "source", true), Row("", "source2", true)),
      expectV2Catalog = false)
  }

  test("ShowTables: using v1 catalog, db doesn't exist ") {
    // 'db' below resolves to a database name for v1 catalog because there is no catalog named
    // 'db' and there is no default catalog set.
    val exception = intercept[NoSuchDatabaseException] {
      runShowTablesSql("SHOW TABLES FROM db", Seq(), expectV2Catalog = false)
    }

    assert(exception.getMessage.contains("Database 'db' not found"))
  }

  test("ShowTables: using v1 catalog, db name with multipartIdentifier ('a.b') is not allowed.") {
    val exception = intercept[AnalysisException] {
      runShowTablesSql("SHOW TABLES FROM a.b", Seq(), expectV2Catalog = false)
    }

    assert(exception.getMessage.contains("The database name is not valid: a.b"))
  }

  test("ShowTables: using v2 catalog with empty namespace") {
    spark.sql("CREATE TABLE testcat.table (id bigint, data string) USING foo")
    runShowTablesSql("SHOW TABLES FROM testcat", Seq(Row("", "table")))
  }

  test("ShowTables: namespace is not specified and default v2 catalog is set") {
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "testcat")
    spark.sql("CREATE TABLE testcat.table (id bigint, data string) USING foo")

    // v2 catalog is used where default namespace is empty for TestInMemoryTableCatalog.
    runShowTablesSql("SHOW TABLES", Seq(Row("", "table")))
  }

  test("ShowTables: namespace not specified and default v2 catalog not set - fallback to v1") {
    runShowTablesSql(
      "SHOW TABLES",
      Seq(Row("", "source", true), Row("", "source2", true)),
      expectV2Catalog = false)

    runShowTablesSql(
      "SHOW TABLES LIKE '*2'",
      Seq(Row("", "source2", true)),
      expectV2Catalog = false)
  }

  test("ShowTables: change current catalog and namespace with USE statements") {
    sql("CREATE TABLE testcat.ns1.ns2.table (id bigint) USING foo")

    // Initially, the v2 session catalog (current catalog) is used.
    runShowTablesSql(
      "SHOW TABLES", Seq(Row("", "source", true), Row("", "source2", true)),
      expectV2Catalog = false)

    // Update the current catalog, and no table is matched since the current namespace is Array().
    sql("USE testcat")
    runShowTablesSql("SHOW TABLES", Seq())

    // Update the current namespace to match ns1.ns2.table.
    sql("USE testcat.ns1.ns2")
    runShowTablesSql("SHOW TABLES", Seq(Row("ns1.ns2", "table")))
  }

  private def runShowTablesSql(
      sqlText: String,
      expected: Seq[Row],
      expectV2Catalog: Boolean = true): Unit = {
    val schema = if (expectV2Catalog) {
      new StructType()
        .add("namespace", StringType, nullable = false)
        .add("tableName", StringType, nullable = false)
    } else {
      new StructType()
        .add("database", StringType, nullable = false)
        .add("tableName", StringType, nullable = false)
        .add("isTemporary", BooleanType, nullable = false)
    }

    val df = spark.sql(sqlText)
    assert(df.schema === schema)
    assert(expected === df.collect())
  }

  test("SHOW TABLE EXTENDED not valid v1 database") {
    def testV1CommandNamespace(sqlCommand: String, namespace: String): Unit = {
      val e = intercept[AnalysisException] {
        sql(sqlCommand)
      }
      assert(e.message.contains(s"The database name is not valid: ${namespace}"))
    }

    val namespace = "testcat.ns1.ns2"
    val table = "tbl"
    withTable(s"$namespace.$table") {
      sql(s"CREATE TABLE $namespace.$table (id bigint, data string) " +
        s"USING foo PARTITIONED BY (id)")

      testV1CommandNamespace(s"SHOW TABLE EXTENDED FROM $namespace LIKE 'tb*'",
        namespace)
      testV1CommandNamespace(s"SHOW TABLE EXTENDED IN $namespace LIKE 'tb*'",
        namespace)
      testV1CommandNamespace("SHOW TABLE EXTENDED " +
        s"FROM $namespace LIKE 'tb*' PARTITION(id=1)",
        namespace)
      testV1CommandNamespace("SHOW TABLE EXTENDED " +
        s"IN $namespace LIKE 'tb*' PARTITION(id=1)",
        namespace)
    }
  }

  test("SHOW TABLE EXTENDED valid v1") {
    val expected = Seq(Row("", "source", true), Row("", "source2", true))
    val schema = new StructType()
      .add("database", StringType, nullable = false)
      .add("tableName", StringType, nullable = false)
      .add("isTemporary", BooleanType, nullable = false)
      .add("information", StringType, nullable = false)

    val df = sql("SHOW TABLE EXTENDED FROM default LIKE '*source*'")
    val result = df.collect()
    val resultWithoutInfo = result.map{ case Row(db, table, temp, _) => Row(db, table, temp)}

    assert(df.schema === schema)
    assert(resultWithoutInfo === expected)
    result.foreach{ case Row(_, _, _, info: String) => assert(info.nonEmpty)}
  }

  test("CreateNameSpace: basic tests") {
    // Session catalog is used.
    withNamespace("ns") {
      sql("CREATE NAMESPACE ns")
      testShowNamespaces("SHOW NAMESPACES", Seq("default", "ns"))
    }

    // V2 non-session catalog is used.
    withNamespace("testcat.ns1.ns2") {
      sql("CREATE NAMESPACE testcat.ns1.ns2")
      testShowNamespaces("SHOW NAMESPACES IN testcat", Seq("ns1"))
      testShowNamespaces("SHOW NAMESPACES IN testcat.ns1", Seq("ns1.ns2"))
    }

    withNamespace("testcat.test") {
      withTempDir { tmpDir =>
        val path = tmpDir.getCanonicalPath
        sql(s"CREATE NAMESPACE testcat.test LOCATION '$path'")
        val metadata =
          catalog("testcat").asNamespaceCatalog.loadNamespaceMetadata(Array("test")).asScala
        val catalogPath = metadata(V2SessionCatalog.LOCATION_TABLE_PROP)
        assert(catalogPath.equals(catalogPath))
      }
    }
  }

  test("CreateNameSpace: test handling of 'IF NOT EXIST'") {
    withNamespace("testcat.ns1") {
      sql("CREATE NAMESPACE IF NOT EXISTS testcat.ns1")

      // The 'ns1' namespace already exists, so this should fail.
      val exception = intercept[NamespaceAlreadyExistsException] {
        sql("CREATE NAMESPACE testcat.ns1")
      }
      assert(exception.getMessage.contains("Namespace 'ns1' already exists"))

      // The following will be no-op since the namespace already exists.
      sql("CREATE NAMESPACE IF NOT EXISTS testcat.ns1")
    }
  }

  test("DropNamespace: basic tests") {
    // Session catalog is used.
    sql("CREATE NAMESPACE ns")
    testShowNamespaces("SHOW NAMESPACES", Seq("default", "ns"))
    sql("DROP NAMESPACE ns")
    testShowNamespaces("SHOW NAMESPACES", Seq("default"))

    // V2 non-session catalog is used.
    sql("CREATE NAMESPACE testcat.ns1")
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq("ns1"))
    sql("DROP NAMESPACE testcat.ns1")
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq())
  }

  test("DropNamespace: drop non-empty namespace with a non-cascading mode") {
    sql("CREATE TABLE testcat.ns1.table (id bigint) USING foo")
    sql("CREATE TABLE testcat.ns1.ns2.table (id bigint) USING foo")
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq("ns1"))
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns1", Seq("ns1.ns2"))

    def assertDropFails(): Unit = {
      val e = intercept[SparkException] {
        sql("DROP NAMESPACE testcat.ns1")
      }
      assert(e.getMessage.contains("Cannot drop a non-empty namespace: ns1"))
    }

    // testcat.ns1.table is present, thus testcat.ns1 cannot be dropped.
    assertDropFails()
    sql("DROP TABLE testcat.ns1.table")

    // testcat.ns1.ns2.table is present, thus testcat.ns1 cannot be dropped.
    assertDropFails()
    sql("DROP TABLE testcat.ns1.ns2.table")

    // testcat.ns1.ns2 namespace is present, thus testcat.ns1 cannot be dropped.
    assertDropFails()
    sql("DROP NAMESPACE testcat.ns1.ns2")

    // Now that testcat.ns1 is empty, it can be dropped.
    sql("DROP NAMESPACE testcat.ns1")
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq())
  }

  test("DropNamespace: drop non-empty namespace with a cascade mode") {
    sql("CREATE TABLE testcat.ns1.table (id bigint) USING foo")
    sql("CREATE TABLE testcat.ns1.ns2.table (id bigint) USING foo")
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq("ns1"))
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns1", Seq("ns1.ns2"))

    sql("DROP NAMESPACE testcat.ns1 CASCADE")
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq())
  }

  test("DropNamespace: test handling of 'IF EXISTS'") {
    sql("DROP NAMESPACE IF EXISTS testcat.unknown")

    val exception = intercept[NoSuchNamespaceException] {
      sql("DROP NAMESPACE testcat.ns1")
    }
    assert(exception.getMessage.contains("Namespace 'ns1' not found"))
  }

  test("DescribeNamespace using v2 catalog") {
    withNamespace("testcat.ns1.ns2") {
      sql("CREATE NAMESPACE IF NOT EXISTS testcat.ns1.ns2 COMMENT " +
        "'test namespace' LOCATION '/tmp/ns_test'")
      val descriptionDf = sql("DESCRIBE NAMESPACE testcat.ns1.ns2")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) ===
        Seq(
          ("name", StringType),
          ("value", StringType)
        ))
      val description = descriptionDf.collect()
      assert(description === Seq(
        Row("Namespace Name", "ns2"),
        Row("Description", "test namespace"),
        Row("Location", "/tmp/ns_test")
      ))
    }
  }

  test("AlterNamespaceSetProperties using v2 catalog") {
    withNamespace("testcat.ns1.ns2") {
      sql("CREATE NAMESPACE IF NOT EXISTS testcat.ns1.ns2 COMMENT " +
        "'test namespace' LOCATION '/tmp/ns_test' WITH PROPERTIES ('a'='a','b'='b','c'='c')")
      sql("ALTER NAMESPACE testcat.ns1.ns2 SET PROPERTIES ('a'='b','b'='a')")
      val descriptionDf = sql("DESCRIBE NAMESPACE EXTENDED testcat.ns1.ns2")
      assert(descriptionDf.collect() === Seq(
        Row("Namespace Name", "ns2"),
        Row("Description", "test namespace"),
        Row("Location", "/tmp/ns_test"),
        Row("Properties", "((a,b),(b,a),(c,c))")
      ))
    }
  }

  test("AlterNamespaceSetLocation using v2 catalog") {
    withNamespace("testcat.ns1.ns2") {
      sql("CREATE NAMESPACE IF NOT EXISTS testcat.ns1.ns2 COMMENT " +
        "'test namespace' LOCATION '/tmp/ns_test_1'")
      sql("ALTER NAMESPACE testcat.ns1.ns2 SET LOCATION '/tmp/ns_test_2'")
      val descriptionDf = sql("DESCRIBE NAMESPACE EXTENDED testcat.ns1.ns2")
      assert(descriptionDf.collect() === Seq(
        Row("Namespace Name", "ns2"),
        Row("Description", "test namespace"),
        Row("Location", "/tmp/ns_test_2")
      ))
    }
  }

  test("ShowNamespaces: show root namespaces with default v2 catalog") {
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "testcat")

    testShowNamespaces("SHOW NAMESPACES", Seq())

    spark.sql("CREATE TABLE testcat.ns1.table (id bigint) USING foo")
    spark.sql("CREATE TABLE testcat.ns1.ns1_1.table (id bigint) USING foo")
    spark.sql("CREATE TABLE testcat.ns2.table (id bigint) USING foo")

    testShowNamespaces("SHOW NAMESPACES", Seq("ns1", "ns2"))
    testShowNamespaces("SHOW NAMESPACES LIKE '*1*'", Seq("ns1"))
  }

  test("ShowNamespaces: show namespaces with v2 catalog") {
    spark.sql("CREATE TABLE testcat.ns1.table (id bigint) USING foo")
    spark.sql("CREATE TABLE testcat.ns1.ns1_1.table (id bigint) USING foo")
    spark.sql("CREATE TABLE testcat.ns1.ns1_2.table (id bigint) USING foo")
    spark.sql("CREATE TABLE testcat.ns2.table (id bigint) USING foo")
    spark.sql("CREATE TABLE testcat.ns2.ns2_1.table (id bigint) USING foo")

    // Look up only with catalog name, which should list root namespaces.
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq("ns1", "ns2"))

    // Look up sub-namespaces.
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns1", Seq("ns1.ns1_1", "ns1.ns1_2"))
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns1 LIKE '*2*'", Seq("ns1.ns1_2"))
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns2", Seq("ns2.ns2_1"))

    // Try to look up namespaces that do not exist.
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns3", Seq())
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns1.ns3", Seq())
  }

  test("ShowNamespaces: default v2 catalog is not set") {
    spark.sql("CREATE TABLE testcat.ns.table (id bigint) USING foo")

    // The current catalog is resolved to a v2 session catalog.
    testShowNamespaces("SHOW NAMESPACES", Seq("default"))
  }

  test("ShowNamespaces: default v2 catalog doesn't support namespace") {
    spark.conf.set(
      "spark.sql.catalog.testcat_no_namspace",
      classOf[BasicInMemoryTableCatalog].getName)
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "testcat_no_namspace")

    val exception = intercept[AnalysisException] {
      sql("SHOW NAMESPACES")
    }

    assert(exception.getMessage.contains("does not support namespaces"))
  }

  test("ShowNamespaces: v2 catalog doesn't support namespace") {
    spark.conf.set(
      "spark.sql.catalog.testcat_no_namspace",
      classOf[BasicInMemoryTableCatalog].getName)

    val exception = intercept[AnalysisException] {
      sql("SHOW NAMESPACES in testcat_no_namspace")
    }

    assert(exception.getMessage.contains("does not support namespaces"))
  }

  test("ShowNamespaces: session catalog is used and namespace doesn't exist") {
    val exception = intercept[AnalysisException] {
      sql("SHOW NAMESPACES in dummy")
    }

    assert(exception.getMessage.contains("Namespace 'dummy' not found"))
  }

  test("ShowNamespaces: change catalog and namespace with USE statements") {
    sql("CREATE TABLE testcat.ns1.ns2.table (id bigint) USING foo")

    // Initially, the current catalog is a v2 session catalog.
    testShowNamespaces("SHOW NAMESPACES", Seq("default"))

    // Update the current catalog to 'testcat'.
    sql("USE testcat")
    testShowNamespaces("SHOW NAMESPACES", Seq("ns1"))

    // Update the current namespace to 'ns1'.
    sql("USE ns1")
    // 'SHOW NAMESPACES' is not affected by the current namespace and lists root namespaces.
    testShowNamespaces("SHOW NAMESPACES", Seq("ns1"))
  }

  private def testShowNamespaces(
      sqlText: String,
      expected: Seq[String]): Unit = {
    val schema = new StructType().add("namespace", StringType, nullable = false)

    val df = spark.sql(sqlText)
    assert(df.schema === schema)
    assert(df.collect().map(_.getAs[String](0)).sorted === expected.sorted)
  }

  test("Use: basic tests with USE statements") {
    val catalogManager = spark.sessionState.catalogManager

    // Validate the initial current catalog and namespace.
    assert(catalogManager.currentCatalog.name() == SESSION_CATALOG_NAME)
    assert(catalogManager.currentNamespace === Array("default"))

    // The following implicitly creates namespaces.
    sql("CREATE TABLE testcat.ns1.ns1_1.table (id bigint) USING foo")
    sql("CREATE TABLE testcat2.ns2.ns2_2.table (id bigint) USING foo")
    sql("CREATE TABLE testcat2.ns3.ns3_3.table (id bigint) USING foo")
    sql("CREATE TABLE testcat2.testcat.table (id bigint) USING foo")

    // Catalog is resolved to 'testcat'.
    sql("USE testcat.ns1.ns1_1")
    assert(catalogManager.currentCatalog.name() == "testcat")
    assert(catalogManager.currentNamespace === Array("ns1", "ns1_1"))

    // Catalog is resolved to 'testcat2'.
    sql("USE testcat2.ns2.ns2_2")
    assert(catalogManager.currentCatalog.name() == "testcat2")
    assert(catalogManager.currentNamespace === Array("ns2", "ns2_2"))

    // Only the namespace is changed.
    sql("USE ns3.ns3_3")
    assert(catalogManager.currentCatalog.name() == "testcat2")
    assert(catalogManager.currentNamespace === Array("ns3", "ns3_3"))

    // Only the namespace is changed (explicit).
    sql("USE NAMESPACE testcat")
    assert(catalogManager.currentCatalog.name() == "testcat2")
    assert(catalogManager.currentNamespace === Array("testcat"))

    // Catalog is resolved to `testcat`.
    sql("USE testcat")
    assert(catalogManager.currentCatalog.name() == "testcat")
    assert(catalogManager.currentNamespace === Array())
  }

  test("Use: set v2 catalog as a current catalog") {
    val catalogManager = spark.sessionState.catalogManager
    assert(catalogManager.currentCatalog.name() == SESSION_CATALOG_NAME)

    sql("USE testcat")
    assert(catalogManager.currentCatalog.name() == "testcat")
  }

  test("Use: v2 session catalog is used and namespace does not exist") {
    val exception = intercept[NoSuchDatabaseException] {
      sql("USE ns1")
    }
    assert(exception.getMessage.contains("Database 'ns1' not found"))
  }

  test("Use: v2 catalog is used and namespace does not exist") {
    // Namespaces are not required to exist for v2 catalogs.
    sql("USE testcat.ns1.ns2")
    val catalogManager = spark.sessionState.catalogManager
    assert(catalogManager.currentNamespace === Array("ns1", "ns2"))
  }

  test("ShowCurrentNamespace: basic tests") {
    def testShowCurrentNamespace(expectedCatalogName: String, expectedNamespace: String): Unit = {
      val schema = new StructType()
        .add("catalog", StringType, nullable = false)
        .add("namespace", StringType, nullable = false)
      val df = sql("SHOW CURRENT NAMESPACE")
      val rows = df.collect

      assert(df.schema === schema)
      assert(rows.length == 1)
      assert(rows(0).getAs[String](0) === expectedCatalogName)
      assert(rows(0).getAs[String](1) === expectedNamespace)
    }

    // Initially, the v2 session catalog is set as a current catalog.
    testShowCurrentNamespace("spark_catalog", "default")

    sql("USE testcat")
    testShowCurrentNamespace("testcat", "")
    sql("USE testcat.ns1.ns2")
    testShowCurrentNamespace("testcat", "ns1.ns2")
  }

  test("tableCreation: partition column case insensitive resolution") {
    val testCatalog = catalog("testcat").asTableCatalog
    val sessionCatalog = catalog(SESSION_CATALOG_NAME).asTableCatalog

    def checkPartitioning(cat: TableCatalog, partition: String): Unit = {
      val table = cat.loadTable(Identifier.of(Array.empty, "tbl"))
      val partitions = table.partitioning().map(_.references())
      assert(partitions.length === 1)
      val fieldNames = partitions.flatMap(_.map(_.fieldNames()))
      assert(fieldNames === Array(Array(partition)))
    }

    sql(s"CREATE TABLE tbl (a int, b string) USING $v2Source PARTITIONED BY (A)")
    checkPartitioning(sessionCatalog, "a")
    sql(s"CREATE TABLE testcat.tbl (a int, b string) USING $v2Source PARTITIONED BY (A)")
    checkPartitioning(testCatalog, "a")
    sql(s"CREATE OR REPLACE TABLE tbl (a int, b string) USING $v2Source PARTITIONED BY (B)")
    checkPartitioning(sessionCatalog, "b")
    sql(s"CREATE OR REPLACE TABLE testcat.tbl (a int, b string) USING $v2Source PARTITIONED BY (B)")
    checkPartitioning(testCatalog, "b")
  }

  test("tableCreation: partition column case sensitive resolution") {
    def checkFailure(statement: String): Unit = {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val e = intercept[AnalysisException] {
          sql(statement)
        }
        assert(e.getMessage.contains("Couldn't find column"))
      }
    }

    checkFailure(s"CREATE TABLE tbl (a int, b string) USING $v2Source PARTITIONED BY (A)")
    checkFailure(s"CREATE TABLE testcat.tbl (a int, b string) USING $v2Source PARTITIONED BY (A)")
    checkFailure(
      s"CREATE OR REPLACE TABLE tbl (a int, b string) USING $v2Source PARTITIONED BY (B)")
    checkFailure(
      s"CREATE OR REPLACE TABLE testcat.tbl (a int, b string) USING $v2Source PARTITIONED BY (B)")
  }

  test("tableCreation: duplicate column names in the table definition") {
    val errorMsg = "Found duplicate column(s) in the table definition of `t`"
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        assertAnalysisError(
          s"CREATE TABLE t ($c0 INT, $c1 INT) USING $v2Source",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE TABLE testcat.t ($c0 INT, $c1 INT) USING $v2Source",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE t ($c0 INT, $c1 INT) USING $v2Source",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE testcat.t ($c0 INT, $c1 INT) USING $v2Source",
          errorMsg
        )
      }
    }
  }

  test("tableCreation: duplicate nested column names in the table definition") {
    val errorMsg = "Found duplicate column(s) in the table definition of `t`"
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        assertAnalysisError(
          s"CREATE TABLE t (d struct<$c0: INT, $c1: INT>) USING $v2Source",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE TABLE testcat.t (d struct<$c0: INT, $c1: INT>) USING $v2Source",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE t (d struct<$c0: INT, $c1: INT>) USING $v2Source",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE testcat.t (d struct<$c0: INT, $c1: INT>) USING $v2Source",
          errorMsg
        )
      }
    }
  }

  test("tableCreation: bucket column names not in table definition") {
    val errorMsg = "Couldn't find column c in"
    assertAnalysisError(
      s"CREATE TABLE tbl (a int, b string) USING $v2Source CLUSTERED BY (c) INTO 4 BUCKETS",
      errorMsg
    )
    assertAnalysisError(
      s"CREATE TABLE testcat.tbl (a int, b string) USING $v2Source CLUSTERED BY (c) INTO 4 BUCKETS",
      errorMsg
    )
    assertAnalysisError(
      s"CREATE OR REPLACE TABLE tbl (a int, b string) USING $v2Source " +
        "CLUSTERED BY (c) INTO 4 BUCKETS",
      errorMsg
    )
    assertAnalysisError(
      s"CREATE OR REPLACE TABLE testcat.tbl (a int, b string) USING $v2Source " +
        "CLUSTERED BY (c) INTO 4 BUCKETS",
      errorMsg
    )
  }

  test("tableCreation: bucket column name containing dot") {
    withTable("t") {
      sql(
        """
          |CREATE TABLE testcat.t (id int, `a.b` string) USING foo
          |CLUSTERED BY (`a.b`) INTO 4 BUCKETS
          |OPTIONS ('allow-unsupported-transforms'=true)
        """.stripMargin)

      val testCatalog = catalog("testcat").asTableCatalog.asInstanceOf[InMemoryTableCatalog]
      val table = testCatalog.loadTable(Identifier.of(Array.empty, "t"))
      val partitioning = table.partitioning()
      assert(partitioning.length == 1 && partitioning.head.name() == "bucket")
      val references = partitioning.head.references()
      assert(references.length == 1)
      assert(references.head.fieldNames().toSeq == Seq("a.b"))
    }
  }

  test("tableCreation: column repeated in partition columns") {
    val errorMsg = "Found duplicate column(s) in the partitioning"
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        assertAnalysisError(
          s"CREATE TABLE t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE TABLE testcat.t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE testcat.t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)",
          errorMsg
        )
      }
    }
  }

  test("tableCreation: column repeated in bucket columns") {
    val errorMsg = "Found duplicate column(s) in the bucket definition"
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        assertAnalysisError(
          s"CREATE TABLE t ($c0 INT) USING $v2Source " +
            s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE TABLE testcat.t ($c0 INT) USING $v2Source " +
            s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE t ($c0 INT) USING $v2Source " +
            s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE testcat.t ($c0 INT) USING $v2Source " +
            s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS",
          errorMsg
        )
      }
    }
  }

  test("REFRESH TABLE: v2 table") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      val testCatalog = catalog("testcat").asTableCatalog.asInstanceOf[InMemoryTableCatalog]
      val identifier = Identifier.of(Array("ns1", "ns2"), "tbl")

      assert(!testCatalog.isTableInvalidated(identifier))
      sql(s"REFRESH TABLE $t")
      assert(testCatalog.isTableInvalidated(identifier))
    }
  }

  test("REPLACE TABLE: v1 table") {
    val e = intercept[AnalysisException] {
      sql(s"CREATE OR REPLACE TABLE tbl (a int) USING ${classOf[SimpleScanSource].getName}")
    }
    assert(e.message.contains("REPLACE TABLE is only supported with v2 tables"))
  }

  test("DeleteFrom: basic - delete all") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t")
      checkAnswer(spark.table(t), Seq())
    }
  }

  test("DeleteFrom: basic - delete with where clause") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t WHERE id = 2")
      checkAnswer(spark.table(t), Seq(
        Row(3, "c", 3)))
    }
  }

  test("DeleteFrom: delete from aliased target table") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t AS tbl WHERE tbl.id = 2")
      checkAnswer(spark.table(t), Seq(
        Row(3, "c", 3)))
    }
  }

  test("DeleteFrom: normalize attribute names") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t AS tbl WHERE tbl.ID = 2")
      checkAnswer(spark.table(t), Seq(
        Row(3, "c", 3)))
    }
  }

  test("DeleteFrom: fail if has subquery") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      val exc = intercept[AnalysisException] {
        sql(s"DELETE FROM $t WHERE id IN (SELECT id FROM $t)")
      }

      assert(spark.table(t).count === 3)
      assert(exc.getMessage.contains("Delete by condition with subquery is not supported"))
    }
  }

  test("DeleteFrom: DELETE is only supported with v2 tables") {
    // unset this config to use the default v2 session catalog.
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
    val v1Table = "tbl"
    withTable(v1Table) {
      sql(s"CREATE TABLE $v1Table" +
          s" USING ${classOf[SimpleScanSource].getName} OPTIONS (from=0,to=1)")
      val exc = intercept[AnalysisException] {
        sql(s"DELETE FROM $v1Table WHERE i = 2")
      }

      assert(exc.getMessage.contains("DELETE is only supported with v2 tables"))
    }
  }

  test("UPDATE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(
        s"""
           |CREATE TABLE $t (id bigint, name string, age int, p int)
           |USING foo
           |PARTITIONED BY (id, p)
         """.stripMargin)

      // UPDATE non-existing table
      assertAnalysisError(
        "UPDATE dummy SET name='abc'",
        "Table or view not found")

      // UPDATE non-existing column
      assertAnalysisError(
        s"UPDATE $t SET dummy='abc'",
        "cannot resolve")
      assertAnalysisError(
        s"UPDATE $t SET name='abc' WHERE dummy=1",
        "cannot resolve")

      // UPDATE is not implemented yet.
      val e = intercept[UnsupportedOperationException] {
        sql(s"UPDATE $t SET name='Robert', age=32 WHERE p=1")
      }
      assert(e.getMessage.contains("UPDATE TABLE is not supported temporarily"))
    }
  }

  test("MERGE INTO TABLE") {
    val target = "testcat.ns1.ns2.target"
    val source = "testcat.ns1.ns2.source"
    withTable(target, source) {
      sql(
        s"""
           |CREATE TABLE $target (id bigint, name string, age int, p int)
           |USING foo
           |PARTITIONED BY (id, p)
         """.stripMargin)
      sql(
        s"""
           |CREATE TABLE $source (id bigint, name string, age int, p int)
           |USING foo
           |PARTITIONED BY (id, p)
         """.stripMargin)

      // MERGE INTO non-existing table
      assertAnalysisError(
        s"""
           |MERGE INTO testcat.ns1.ns2.dummy AS target
           |USING testcat.ns1.ns2.source AS source
           |ON target.id = source.id
           |WHEN MATCHED AND (target.age < 10) THEN DELETE
           |WHEN MATCHED AND (target.age > 10) THEN UPDATE SET *
           |WHEN NOT MATCHED AND (target.col2='insert')
           |THEN INSERT *
         """.stripMargin,
        "Table or view not found")

      // USING non-existing table
      assertAnalysisError(
        s"""
           |MERGE INTO testcat.ns1.ns2.target AS target
           |USING testcat.ns1.ns2.dummy AS source
           |ON target.id = source.id
           |WHEN MATCHED AND (target.age < 10) THEN DELETE
           |WHEN MATCHED AND (target.age > 10) THEN UPDATE SET *
           |WHEN NOT MATCHED AND (target.col2='insert')
           |THEN INSERT *
         """.stripMargin,
        "Table or view not found")

      // UPDATE non-existing column
      assertAnalysisError(
        s"""
           |MERGE INTO testcat.ns1.ns2.target AS target
           |USING testcat.ns1.ns2.source AS source
           |ON target.id = source.id
           |WHEN MATCHED AND (target.age < 10) THEN DELETE
           |WHEN MATCHED AND (target.age > 10) THEN UPDATE SET target.dummy = source.age
           |WHEN NOT MATCHED AND (target.col2='insert')
           |THEN INSERT *
         """.stripMargin,
        "cannot resolve")

      // UPDATE using non-existing column
      assertAnalysisError(
        s"""
           |MERGE INTO testcat.ns1.ns2.target AS target
           |USING testcat.ns1.ns2.source AS source
           |ON target.id = source.id
           |WHEN MATCHED AND (target.age < 10) THEN DELETE
           |WHEN MATCHED AND (target.age > 10) THEN UPDATE SET target.age = source.dummy
           |WHEN NOT MATCHED AND (target.col2='insert')
           |THEN INSERT *
         """.stripMargin,
        "cannot resolve")

      // MERGE INTO is not implemented yet.
      val e = intercept[UnsupportedOperationException] {
        sql(
          s"""
             |MERGE INTO testcat.ns1.ns2.target AS target
             |USING testcat.ns1.ns2.source AS source
             |ON target.id = source.id
             |WHEN MATCHED AND (target.p < 0) THEN DELETE
             |WHEN MATCHED AND (target.p > 0) THEN UPDATE SET *
             |WHEN NOT MATCHED THEN INSERT *
           """.stripMargin)
      }
      assert(e.getMessage.contains("MERGE INTO TABLE is not supported temporarily"))
    }
  }

  test("AlterTable: rename table basic test") {
    withTable("testcat.ns1.new") {
      sql(s"CREATE TABLE testcat.ns1.ns2.old USING foo AS SELECT id, data FROM source")
      checkAnswer(sql("SHOW TABLES FROM testcat.ns1.ns2"), Seq(Row("ns1.ns2", "old")))

      sql(s"ALTER TABLE testcat.ns1.ns2.old RENAME TO ns1.new")
      checkAnswer(sql("SHOW TABLES FROM testcat.ns1.ns2"), Seq.empty)
      checkAnswer(sql("SHOW TABLES FROM testcat.ns1"), Seq(Row("ns1", "new")))
    }
  }

  test("AlterTable: renaming views are not supported") {
    val e = intercept[AnalysisException] {
      sql(s"ALTER VIEW testcat.ns.tbl RENAME TO ns.view")
    }
    assert(e.getMessage.contains("Renaming view is not supported in v2 catalogs"))
  }

  test("ANALYZE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      testV1Command("ANALYZE TABLE", s"$t COMPUTE STATISTICS")
      testV1Command("ANALYZE TABLE", s"$t COMPUTE STATISTICS FOR ALL COLUMNS")
    }
  }

  test("MSCK REPAIR TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      testV1Command("MSCK REPAIR TABLE", t)
    }
  }

  test("TRUNCATE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(
        s"""
           |CREATE TABLE $t (id bigint, data string)
           |USING foo
           |PARTITIONED BY (id)
         """.stripMargin)

      testV1Command("TRUNCATE TABLE", t)
      testV1Command("TRUNCATE TABLE", s"$t PARTITION(id='1')")
    }
  }

  test("SHOW PARTITIONS") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(
        s"""
           |CREATE TABLE $t (id bigint, data string)
           |USING foo
           |PARTITIONED BY (id)
         """.stripMargin)

      testV1Command("SHOW PARTITIONS", t)
      testV1Command("SHOW PARTITIONS", s"$t PARTITION(id='1')")
    }
  }

  test("LOAD DATA INTO TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(
        s"""
           |CREATE TABLE $t (id bigint, data string)
           |USING foo
           |PARTITIONED BY (id)
         """.stripMargin)

      testV1Command("LOAD DATA", s"INPATH 'filepath' INTO TABLE $t")
      testV1Command("LOAD DATA", s"LOCAL INPATH 'filepath' INTO TABLE $t")
      testV1Command("LOAD DATA", s"LOCAL INPATH 'filepath' OVERWRITE INTO TABLE $t")
      testV1Command("LOAD DATA",
        s"LOCAL INPATH 'filepath' OVERWRITE INTO TABLE $t PARTITION(id=1)")
    }
  }

  test("SHOW CREATE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      testV1Command("SHOW CREATE TABLE", t)
    }
  }

  test("CACHE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      testV1Command("CACHE TABLE", t)

      val e = intercept[AnalysisException] {
        sql(s"CACHE LAZY TABLE $t")
      }
      assert(e.message.contains("CACHE TABLE is only supported with v1 tables"))
    }
  }

  test("UNCACHE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      testV1Command("UNCACHE TABLE", t)
      testV1Command("UNCACHE TABLE", s"IF EXISTS $t")
    }
  }

  test("SHOW COLUMNS") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      testV1Command("SHOW COLUMNS", s"FROM $t")
      testV1Command("SHOW COLUMNS", s"IN $t")

      val e3 = intercept[AnalysisException] {
        sql(s"SHOW COLUMNS FROM tbl IN testcat.ns1.ns2")
      }
      assert(e3.message.contains("Namespace name should have " +
        "only one part if specified: testcat.ns1.ns2"))
    }
  }

  test("ALTER TABLE RECOVER PARTITIONS") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t RECOVER PARTITIONS")
      }
      assert(e.message.contains("ALTER TABLE RECOVER PARTITIONS is only supported with v1 tables"))
    }
  }

  test("ALTER TABLE ADD PARTITION") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'")
      }
      assert(e.message.contains("ALTER TABLE ADD PARTITION is only supported with v1 tables"))
    }
  }

  test("ALTER TABLE RENAME PARTITION") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t PARTITION (id=1) RENAME TO PARTITION (id=2)")
      }
      assert(e.message.contains("ALTER TABLE RENAME PARTITION is only supported with v1 tables"))
    }
  }

  test("ALTER TABLE DROP PARTITIONS") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP PARTITION (id=1)")
      }
      assert(e.message.contains("ALTER TABLE DROP PARTITION is only supported with v1 tables"))
    }
  }

  test("ALTER TABLE SerDe properties") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')")
      }
      assert(e.message.contains("ALTER TABLE SerDe Properties is only supported with v1 tables"))
    }
  }

  test("ALTER VIEW AS QUERY") {
    val v = "testcat.ns1.ns2.v"
    val e = intercept[AnalysisException] {
      sql(s"ALTER VIEW $v AS SELECT 1")
    }
    assert(e.message.contains("ALTER VIEW QUERY is only supported with v1 tables"))
  }

  test("SHOW TBLPROPERTIES: v2 table") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      val owner = "andrew"
      val status = "new"
      val provider = "foo"
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING $provider " +
        s"TBLPROPERTIES ('owner'='$owner', 'status'='$status')")

      val properties = sql(s"SHOW TBLPROPERTIES $t")

      val schema = new StructType()
        .add("key", StringType, nullable = false)
        .add("value", StringType, nullable = false)

      val expected = Seq(
        Row("owner", owner),
        Row("provider", provider),
        Row("status", status))

      assert(properties.schema === schema)
      assert(expected === properties.collect())
    }
  }

  test("SHOW TBLPROPERTIES(key): v2 table") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      val owner = "andrew"
      val status = "new"
      val provider = "foo"
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING $provider " +
        s"TBLPROPERTIES ('owner'='$owner', 'status'='$status')")

      val properties = sql(s"SHOW TBLPROPERTIES $t ('status')")

      val expected = Seq(Row("status", status))

      assert(expected === properties.collect())
    }
  }

  test("SHOW TBLPROPERTIES(key): v2 table, key not found") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      val nonExistingKey = "nonExistingKey"
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo " +
        s"TBLPROPERTIES ('owner'='andrew', 'status'='new')")

      val properties = sql(s"SHOW TBLPROPERTIES $t ('$nonExistingKey')")

      val expected = Seq(Row(nonExistingKey, s"Table $t does not have property: $nonExistingKey"))

      assert(expected === properties.collect())
    }
  }

  test("global temp view should not be masked by v2 catalog") {
    val globalTempDB = spark.sessionState.conf.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
    spark.conf.set(s"spark.sql.catalog.$globalTempDB", classOf[InMemoryTableCatalog].getName)

    try {
      sql("create global temp view v as select 1")
      sql(s"alter view $globalTempDB.v rename to v2")
      checkAnswer(spark.table(s"$globalTempDB.v2"), Row(1))
      sql(s"drop view $globalTempDB.v2")
    } finally {
      spark.sharedState.globalTempViewManager.clear()
    }
  }

  private def testV1Command(sqlCommand: String, sqlParams: String): Unit = {
    val e = intercept[AnalysisException] {
      sql(s"$sqlCommand $sqlParams")
    }
    assert(e.message.contains(s"$sqlCommand is only supported with v1 tables"))
  }

  private def assertAnalysisError(sqlStatement: String, expectedError: String): Unit = {
    val errMsg = intercept[AnalysisException] {
      sql(sqlStatement)
    }.getMessage
    assert(errMsg.contains(expectedError))
  }
}


/** Used as a V2 DataSource for V2SessionCatalog DDL */
class FakeV2Provider extends TableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    throw new UnsupportedOperationException("Unnecessary for DDL tests")
  }
}
