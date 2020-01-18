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

package org.apache.spark.sql.util

import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.connector.expressions.{BucketTransform, FieldReference, NamedTransform, Transform}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}


/**
 * Utils for handling schemas.
 *
 * TODO: Merge this file with [[org.apache.spark.ml.util.SchemaUtils]].
 */
private[spark] object SchemaUtils {

  /**
   * Checks if an input schema has duplicate column names. This throws an exception if the
   * duplication exists.
   *
   * @param schema schema to check
   * @param colType column type name, used in an exception message
   * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not
   */
  def checkSchemaColumnNameDuplication(
      schema: StructType, colType: String, caseSensitiveAnalysis: Boolean = false): Unit = {
    checkColumnNameDuplication(schema.map(_.name), colType, caseSensitiveAnalysis)
  }

  // Returns true if a given resolver is case-sensitive
  private def isCaseSensitiveAnalysis(resolver: Resolver): Boolean = {
    if (resolver == caseSensitiveResolution) {
      true
    } else if (resolver == caseInsensitiveResolution) {
      false
    } else {
      sys.error("A resolver to check if two identifiers are equal must be " +
        "`caseSensitiveResolution` or `caseInsensitiveResolution` in o.a.s.sql.catalyst.")
    }
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param columnNames column names to check
   * @param colType column type name, used in an exception message
   * @param resolver resolver used to determine if two identifiers are equal
   */
  def checkColumnNameDuplication(
      columnNames: Seq[String], colType: String, resolver: Resolver): Unit = {
    checkColumnNameDuplication(columnNames, colType, isCaseSensitiveAnalysis(resolver))
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param columnNames column names to check
   * @param colType column type name, used in an exception message
   * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not
   */
  def checkColumnNameDuplication(
      columnNames: Seq[String], colType: String, caseSensitiveAnalysis: Boolean): Unit = {
    val names = if (caseSensitiveAnalysis) columnNames else columnNames.map(_.toLowerCase)
    if (names.distinct.length != names.length) {
      val duplicateColumns = names.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"`$x`"
      }
      throw new AnalysisException(
        s"Found duplicate column(s) $colType: ${duplicateColumns.mkString(", ")}")
    }
  }

  /**
   * Returns all column names in this schema as a flat list. For example, a schema like:
   *   | - a
   *   | | - 1
   *   | | - 2
   *   | - b
   *   | - c
   *   | | - nest
   *   |   | - 3
   *   will get flattened to: "a", "a.1", "a.2", "b", "c", "c.nest", "c.nest.3"
   */
  def explodeNestedFieldNames(schema: StructType): Seq[String] = {
    def explode(schema: StructType): Seq[Seq[String]] = {
      def recurseIntoComplexTypes(complexType: DataType): Seq[Seq[String]] = {
        complexType match {
          case s: StructType => explode(s)
          case a: ArrayType => recurseIntoComplexTypes(a.elementType)
          case m: MapType =>
            recurseIntoComplexTypes(m.keyType).map(Seq("key") ++ _) ++
                recurseIntoComplexTypes(m.valueType).map(Seq("value") ++ _)
          case _ => Nil
        }
      }

      schema.flatMap {
        case StructField(name, s: StructType, _, _) =>
          Seq(Seq(name)) ++ explode(s).map(nested => Seq(name) ++ nested)
        case StructField(name, a: ArrayType, _, _) =>
          Seq(Seq(name)) ++ recurseIntoComplexTypes(a).map(nested => Seq(name) ++ nested)
        case StructField(name, m: MapType, _, _) =>
          Seq(Seq(name)) ++ recurseIntoComplexTypes(m).map(nested => Seq(name) ++ nested)
        case f => Seq(f.name) :: Nil
      }
    }

    explode(schema).map(UnresolvedAttribute.apply(_).name)
  }

  /**
   * Checks if the partitioning transforms are being duplicated or not. Throws an exception if
   * duplication exists.
   *
   * @param transforms the schema to check for duplicates
   * @param checkType contextual information around the check, used in an exception message
   * @param isCaseSensitive Whether to be case sensitive when comparing column names
   */
  def checkTransformDuplication(
      transforms: Seq[Transform],
      checkType: String,
      isCaseSensitive: Boolean): Unit = {
    val extractedTransforms = transforms.map {
      case b: BucketTransform =>
        val colNames = b.columns.map(c => UnresolvedAttribute(c.fieldNames()).name)
        // We need to check that we're not duplicating columns within our bucketing transform
        checkColumnNameDuplication(colNames, "in the bucket definition", isCaseSensitive)
        b.name -> colNames
      case NamedTransform(transformName, refs) =>
        val fieldNameParts =
          refs.collect { case FieldReference(parts) => UnresolvedAttribute(parts).name }
        // We could also check that we're not duplicating column names here as well if
        // fieldNameParts.length > 1, but we're specifically not, because certain transforms can
        // be defined where this is a legitimate use case.
        transformName -> fieldNameParts
    }
    val normalizedTransforms = if (isCaseSensitive) {
      extractedTransforms
    } else {
      extractedTransforms.map(t => t._1 -> t._2.map(_.toLowerCase(Locale.ROOT)))
    }

    if (normalizedTransforms.distinct.length != normalizedTransforms.length) {
      val duplicateColumns = normalizedTransforms.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"${x._2.mkString(".")}"
      }
      throw new AnalysisException(
        s"Found duplicate column(s) $checkType: ${duplicateColumns.mkString(", ")}")
    }
  }

  /**
   * Returns the given column's ordinal within the given `schema`. The length of the returned
   * position will be as long as how nested the column is.
   *
   * @param column The column to search for in the given struct. If the length of `column` is
   *               greater than 1, we expect to enter a nested field.
   * @param schema The current struct we are looking at.
   * @param resolver The resolver to find the column.
   */
  def findColumnPosition(
      column: Seq[String],
      schema: StructType,
      resolver: Resolver): Seq[Int] = {
    def find(column: Seq[String], schema: StructType, stack: Seq[String]): Seq[Int] = {
      if (column.isEmpty) return Nil
      val thisCol = column.head
      lazy val columnPath = UnresolvedAttribute(stack :+ thisCol).name
      val pos = schema.indexWhere(f => resolver(f.name, thisCol))
      if (pos == -1) {
        throw new IndexOutOfBoundsException(columnPath)
      }
      val children = schema(pos).dataType match {
        case s: StructType =>
          find(column.tail, s, stack :+ thisCol)
        case ArrayType(s: StructType, _) =>
          find(column.tail, s, stack :+ thisCol)
        case o =>
          if (column.length > 1) {
            throw new AnalysisException(
              s"""Expected $columnPath to be a nested data type, but found $o. Was looking for the
                 |index of ${UnresolvedAttribute(column).name} in a nested field
              """.stripMargin)
          }
          Nil
      }
      Seq(pos) ++ children
    }

    try {
      find(column, schema, Nil)
    } catch {
      case i: IndexOutOfBoundsException =>
        throw new AnalysisException(
          s"Couldn't find column ${i.getMessage} in:\n${schema.treeString}")
      case e: AnalysisException =>
        throw new AnalysisException(e.getMessage + s":\n${schema.treeString}")
    }
  }

  /**
   * Gets the name of the column in the given position.
   */
  def getColumnName(position: Seq[Int], schema: StructType): Seq[String] = {
    val topLevel = schema(position.head)
    val field = position.tail.foldLeft(Seq(topLevel.name) -> topLevel) {
      case (nameAndField, pos) =>
        nameAndField._2.dataType match {
          case s: StructType =>
            val nowField = s(pos)
            (nameAndField._1 :+ nowField.name) -> nowField
          case ArrayType(s: StructType, _) =>
            val nowField = s(pos)
            (nameAndField._1 :+ nowField.name) -> nowField
          case _ =>
            throw new AnalysisException(
              s"The positions provided ($pos) cannot be resolved in\n${schema.treeString}.")
        }
    }
    field._1
  }
}
