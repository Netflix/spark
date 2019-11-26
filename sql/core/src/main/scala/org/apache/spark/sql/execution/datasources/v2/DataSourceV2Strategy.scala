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
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.{AlterNamespaceSetProperties, CreateNamespace, DescribeNamespace, DescribeTable, DropNamespace, LogicalPlan, SetCatalogAndNamespace, ShowCurrentNamespace, ShowNamespaces, ShowTableProperties, ShowTables}
import org.apache.spark.sql.execution.SparkPlan

object DataSourceV2Strategy extends Strategy with PredicateHelper {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case desc @ DescribeNamespace(catalog, namespace, extended) =>
      DescribeNamespaceExec(desc.output, catalog, namespace, extended) :: Nil

    case desc @ DescribeTable(DataSourceV2Relation(table, _, _), isExtended) =>
      DescribeTableExec(desc.output, table, isExtended) :: Nil

    case AlterNamespaceSetProperties(catalog, namespace, properties) =>
      AlterNamespaceSetPropertiesExec(catalog, namespace, properties) :: Nil

    case CreateNamespace(catalog, namespace, ifNotExists, properties) =>
      CreateNamespaceExec(catalog, namespace, ifNotExists, properties) :: Nil

    case DropNamespace(catalog, namespace, ifExists, cascade) =>
      DropNamespaceExec(catalog, namespace, ifExists, cascade) :: Nil

    case r: ShowNamespaces =>
      ShowNamespacesExec(r.output, r.catalog, r.namespace, r.pattern) :: Nil

    case r : ShowTables =>
      ShowTablesExec(r.output, r.catalog, r.namespace, r.pattern) :: Nil

    case SetCatalogAndNamespace(catalogManager, catalogName, namespace) =>
      SetCatalogAndNamespaceExec(catalogManager, catalogName, namespace) :: Nil

    case r: ShowCurrentNamespace =>
      ShowCurrentNamespaceExec(r.output, r.catalogManager) :: Nil

    case r @ ShowTableProperties(DataSourceV2Relation(table, _, _), propertyKey) =>
      ShowTablePropertiesExec(r.output, table, propertyKey) :: Nil

    case _ => Nil
  }
}
