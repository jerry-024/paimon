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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.KeyAndBucketExtractor;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_ENABLED;

/** {@link CdcDynamicBucketSinkBase} for {@link CdcRecord}. */
public class CdcDynamicBucketSink extends CdcDynamicBucketSinkBase<CdcRecord> {

    private static final long serialVersionUID = 2L;

    public CdcDynamicBucketSink(FileStoreTable table) {
        super(table);
    }

    @Override
    protected KeyAndBucketExtractor<CdcRecord> createExtractor(TableSchema schema) {
        return new CdcRecordKeyAndBucketExtractor(schema);
    }

    @Override
    protected OneInputStreamOperatorFactory<Tuple2<CdcRecord, Integer>, Committable>
            createWriteOperatorFactory(StoreSinkWrite.Provider writeProvider, String commitUser) {
        Options options = table.coreOptions().toConfiguration();
        boolean coordinatorEnabled = options.get(SINK_WRITER_COORDINATOR_ENABLED);
        return coordinatorEnabled
                ? new CdcDynamicBucketWriteOperator.CoordinatedFactory(
                        table, writeProvider, commitUser)
                : new CdcDynamicBucketWriteOperator.Factory(table, writeProvider, commitUser);
    }
}
