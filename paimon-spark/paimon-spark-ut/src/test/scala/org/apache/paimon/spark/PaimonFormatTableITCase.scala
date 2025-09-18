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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.fs.Path
import org.apache.paimon.table.FormatTable

import org.apache.spark.sql.Row

class PaimonFormatTableITCase extends PaimonSparkTestWithRestCatalogBase {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sql("USE paimon")
    sql("CREATE DATABASE IF NOT EXISTS test_db")
    sql("USE test_db")
  }

  test("FormatTable: write and read non-partitioned table") {
    for (format <- Seq("csv")) {
      withTable("format_test") {
        // Create format table using the same pattern as FormatTableTestBase
        sql(
          s"CREATE TABLE format_test (id INT, name STRING, value DOUBLE) USING $format TBLPROPERTIES ('type'='format-table', 'file.compression'='none')")

        // Insert data using our new write implementation
        sql("INSERT INTO format_test VALUES (1, 'Alice', 10.5)")
        sql("INSERT INTO format_test VALUES (2, 'Bob', 20.7)")
        sql("INSERT INTO format_test VALUES (3, 'Charlie', 30.9)")

        // Test reading all data
        checkAnswer(
          sql("SELECT * FROM format_test ORDER BY id"),
          Seq(
            Row(1, "Alice", 10.5),
            Row(2, "Bob", 20.7),
            Row(3, "Charlie", 30.9)
          )
        )

        // Test column projection (using our scan builder)
        checkAnswer(
          sql("SELECT name, value FROM format_test WHERE id = 2"),
          Seq(Row("Bob", 20.7))
        )

        // Test filtering
        checkAnswer(
          sql("SELECT id FROM format_test WHERE value > 15.0 ORDER BY id"),
          Seq(Row(2), Row(3))
        )

        // Verify this is actually a FormatTable
        val table = paimonCatalog.getTable(Identifier.create("test_db", "format_test"))
        assert(
          table.isInstanceOf[FormatTable],
          s"Table should be FormatTable but was ${table.getClass}")
      }
    }
  }
}
