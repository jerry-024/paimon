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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class PaimonPythonFunction implements CatalogFunction {

    private final String name;

    public PaimonPythonFunction(String name) {
        this.name = name;
        File pyFilePath = new File(String.format("%s.py", name));
        try (OutputStream out = new FileOutputStream(pyFilePath)) {
            String code = generateCode();
            out.write(code.getBytes());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String generateCode() {
        return "from pyflink.table.udf import udf\n"
                + "from pyflink.table import DataTypes\n"
                + "@udf(input_types=DataTypes.STRING(), result_type=DataTypes.STRING())\n"
                + "def eval(str):\n"
                + "    return str + str\n";
    }

    @Override
    public String getClassName() {
        return String.format("%s.eval", name);
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
        return true;
    }

    @Override
    public FunctionLanguage getFunctionLanguage() {
        return FunctionLanguage.PYTHON;
    }

    @Override
    public List<ResourceUri> getFunctionResources() {
        return Collections.emptyList();
    }
}
