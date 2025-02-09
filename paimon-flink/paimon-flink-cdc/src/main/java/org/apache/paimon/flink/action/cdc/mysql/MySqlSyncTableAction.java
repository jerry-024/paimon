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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.SyncJobHandler;
import org.apache.paimon.flink.action.cdc.SyncTableActionBase;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.flink.action.cdc.schema.JdbcTableInfo;
import org.apache.paimon.flink.action.cdc.watermark.CdcTimestampExtractor;
import org.apache.paimon.schema.Schema;

import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize one or multiple MySQL tables into one Paimon table.
 *
 * <p>You should specify MySQL source table in {@code mySqlConfig}. See <a
 * href="https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/#connector-options">document
 * of flink-cdc-connectors</a> for detailed keys and values.
 *
 * <p>If the specified Paimon table does not exist, this action will automatically create the table.
 * Its schema will be derived from all specified MySQL tables. If the Paimon table already exists,
 * its schema will be compared against the schema of all specified MySQL tables.
 *
 * <p>This action supports a limited number of schema changes. Currently, the framework can not drop
 * columns, so the behaviors of `DROP` will be ignored, `RENAME` will add a new column. Currently
 * supported schema changes includes:
 *
 * <ul>
 *   <li>Adding columns.
 *   <li>Altering column types. More specifically,
 *       <ul>
 *         <li>altering from a string type (char, varchar, text) to another string type with longer
 *             length,
 *         <li>altering from a binary type (binary, varbinary, blob) to another binary type with
 *             longer length,
 *         <li>altering from an integer type (tinyint, smallint, int, bigint) to another integer
 *             type with wider range,
 *         <li>altering from a floating-point type (float, double) to another floating-point type
 *             with wider range,
 *       </ul>
 *       are supported.
 * </ul>
 */
public class MySqlSyncTableAction extends SyncTableActionBase {

    private JdbcSchemasInfo mySqlSchemasInfo;

    public MySqlSyncTableAction(
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> mySqlConfig) {
        super(database, table, catalogConfig, mySqlConfig, SyncJobHandler.SourceType.MYSQL);
    }

    @Override
    protected Schema retrieveSchema() throws Exception {
        this.mySqlSchemasInfo =
                MySqlActionUtils.getMySqlTableInfos(
                        cdcSourceConfig, monitorTablePredication(), new ArrayList<>(), typeMapping);
        validateMySqlTableInfos(mySqlSchemasInfo);
        JdbcTableInfo tableInfo = mySqlSchemasInfo.mergeAll();
        return tableInfo.schema();
    }

    @Override
    protected MySqlSource<CdcSourceRecord> buildSource() {
        validateRuntimeExecutionMode();
        String tableList =
                String.format(
                        "(%s)\\.(%s)",
                        cdcSourceConfig.get(MySqlSourceOptions.DATABASE_NAME),
                        cdcSourceConfig.get(MySqlSourceOptions.TABLE_NAME));
        return MySqlActionUtils.buildMySqlSource(cdcSourceConfig, tableList, typeMapping);
    }

    @Override
    protected CdcTimestampExtractor createCdcTimestampExtractor() {
        return MySqlActionUtils.createCdcTimestampExtractor();
    }

    private void validateMySqlTableInfos(JdbcSchemasInfo mySqlSchemasInfo) {
        List<Identifier> nonPkTables = mySqlSchemasInfo.nonPkTables();
        checkArgument(
                nonPkTables.isEmpty(),
                "Source tables of MySQL table synchronization job cannot contain table "
                        + "which doesn't have primary keys.\n"
                        + "They are: %s",
                nonPkTables.stream().map(Identifier::getFullName).collect(Collectors.joining(",")));

        checkArgument(
                !mySqlSchemasInfo.pkTables().isEmpty(),
                "No table satisfies the given database name and table name.");
    }

    private Predicate<String> monitorTablePredication() {
        return tableName -> {
            Pattern tableNamePattern =
                    Pattern.compile(cdcSourceConfig.get(MySqlSourceOptions.TABLE_NAME));
            return tableNamePattern.matcher(tableName).matches();
        };
    }
}
