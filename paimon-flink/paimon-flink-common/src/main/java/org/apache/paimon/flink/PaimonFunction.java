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

package org.apache.paimon.flink;

import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.resource.ResourceUri;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** paimon function. */
public class PaimonFunction implements CatalogFunction {

    private final LambdaScalarFunction definition;

    PaimonFunction(LambdaScalarFunction definition) {
        this.definition = definition;
    }

    @Override
    public String getClassName() {
        return this.definition.getClassName();
    }

    @Override
    public CatalogFunction copy() {
        return new FunctionCatalog.InlineCatalogFunction(definition);
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.empty();
    }

    @Override
    public boolean isGeneric() {
        throw new UnsupportedOperationException(
                "This CatalogFunction is a InlineCatalogFunction. This method should not be called.");
    }

    @Override
    public FunctionLanguage getFunctionLanguage() {
        return FunctionLanguage.JAVA;
    }

    @Override
    public List<ResourceUri> getFunctionResources() {
        return Collections.emptyList();
    }

    public FunctionDefinition getDefinition() {
        return definition;
    }
}
