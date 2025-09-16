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

package org.apache.paimon.flink.source;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.RESTCatalogITCaseBase;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.parquet.ParquetFileFormatFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for format table. */
public class FormatTableReadITCaseBase extends RESTCatalogITCaseBase {

    @Test
    public void testParquetFileFormat() throws IOException {
        FileFormatFactory formatFactory = new ParquetFileFormatFactory();
        RowType rowType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("b", DataTypes.INT())
                        .field("c", DataTypes.INT())
                        .build();
        FormatWriterFactory factory =
                (formatFactory.create(
                                new FileFormatFactory.FormatContext(new Options(), 1024, 1024)))
                        .createWriterFactory(rowType);
        InternalRow[] datas = new InternalRow[2];
        datas[0] = GenericRow.of(1, 1, 1);
        datas[1] = GenericRow.of(2, 2, 2);
        String tableName = "format_table_test";
        Identifier identifier = Identifier.create("default", tableName);
        sql(
                "CREATE TABLE %s (a INT, b INT, c INT) WITH ('file.format'='parquet', 'type'='format-table')",
                tableName);
        RESTToken expiredDataToken =
                new RESTToken(
                        ImmutableMap.of(
                                "akId", "akId-expire", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() + 1000_000);
        restCatalogServer.setDataToken(identifier, expiredDataToken);
        sql("INSERT INTO %s VALUES (1, 1, 1), (2, 2, 2)", tableName);
        assertThat(sql("SELECT a FROM %s", tableName))
                .containsExactlyInAnyOrder(Row.of(1), Row.of(2));
        sql("Drop TABLE %s", tableName);
    }
}
