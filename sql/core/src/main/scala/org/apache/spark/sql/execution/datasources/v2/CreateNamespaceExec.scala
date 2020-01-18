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

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.SupportsNamespaces

/**
 * Physical plan node for creating a namespace.
 */
case class CreateNamespaceExec(
    catalog: SupportsNamespaces,
    namespace: Seq[String],
    ifNotExists: Boolean,
    private var properties: Map[String, String])
    extends V2CommandExec {
  override protected def run(): Seq[InternalRow] = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

    val ns = namespace.toArray
    if (!catalog.namespaceExists(ns)) {
      try {
        catalog.createNamespace(ns, properties.asJava)
      } catch {
        case _: NamespaceAlreadyExistsException if ifNotExists =>
          logWarning(s"Namespace ${namespace.quoted} was created concurrently. Ignoring.")
      }
    } else if (!ifNotExists) {
      throw new NamespaceAlreadyExistsException(ns)
    }

    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
