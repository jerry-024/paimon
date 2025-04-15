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
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.resource.ResourceUri;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** paimon file function. */
public class PaimonFileFunction implements CatalogFunction {

    private final ResourceUri resourceUri;
    private final String className;
    private final FunctionLanguage functionLanguage;

    public PaimonFileFunction(
            ResourceUri resourceUri, String className, FunctionLanguage functionLanguage) {
        this.resourceUri = resourceUri;
        this.className = className;
        this.functionLanguage = functionLanguage;
    }

    @Override
    public String getClassName() {
        return className;
    }

    @Override
    public CatalogFunction copy() {
        return this;
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
        return functionLanguage == FunctionLanguage.PYTHON;
    }

    @Override
    public FunctionLanguage getFunctionLanguage() {
        return functionLanguage;
    }

    @Override
    public List<ResourceUri> getFunctionResources() {
        return Collections.singletonList(resourceUri);
    }
}
