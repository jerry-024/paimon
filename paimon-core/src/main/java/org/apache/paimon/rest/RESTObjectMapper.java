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

package org.apache.paimon.rest;

import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.AlterViewRequest;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListViewsResponse;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeJsonParser;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.Module;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import static org.apache.paimon.utils.JsonSerdeUtil.registerJsonObjects;

/** Object mapper for REST request and response. */
public class RESTObjectMapper {

    public static final ObjectMapper OBJECT_MAPPER = RESTObjectMapper.create();

    private static ObjectMapper create() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.registerModule(createPaimonRestJacksonModule());
        mapper.registerModule(new JavaTimeModule());
        preWarm(mapper, DataField.class);
        preWarm(mapper, ListViewsResponse.class);
        preWarm(mapper, ListDatabasesResponse.class);
        preWarm(mapper, AlterTableRequest.class);
        preWarm(mapper, AlterViewRequest.class);
        preWarm(mapper, ErrorResponse.class);
        preWarm(mapper, ConfigResponse.class);
        return mapper;
    }

    private static <T> void preWarm(ObjectMapper mapper, Class<T> type) {
        try {
            mapper.readValue("{}", type);
        } catch (Exception e) {

        }
    }

    private static Module createPaimonRestJacksonModule() {
        SimpleModule module = new SimpleModule("Paimon_REST");
        registerJsonObjects(
                module,
                DataField.class,
                DataField::serializeJson,
                DataTypeJsonParser::parseDataField);
        registerJsonObjects(
                module, DataType.class, DataType::serializeJson, DataTypeJsonParser::parseDataType);
        return module;
    }
}
