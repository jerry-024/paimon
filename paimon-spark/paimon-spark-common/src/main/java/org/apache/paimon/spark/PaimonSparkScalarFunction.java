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

package org.apache.paimon.spark;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.unsafe.types.UTF8String;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class PaimonSparkScalarFunction implements ScalarFunction<Object>, Serializable {
    private static final long serialVersionUID = 1L;
    private final String functionName;
    private final List<DataType> inputTypes;
    private String lambdaExpression;
    private transient Method compiledMethod;

    public PaimonSparkScalarFunction(
            String functionName, List<DataType> inputTypes, String lambdaExpression) {
        this.functionName = functionName;
        this.inputTypes = inputTypes;
        this.lambdaExpression = lambdaExpression;
    }

    @Override
    public DataType[] inputTypes() {
        return inputTypes.toArray(new DataType[inputTypes.size()]);
    }

    @Override
    public DataType resultType() {
        return StringType;
    }

    @Override
    public Object produceResult(InternalRow input) {
        try {
            if (this.compiledMethod == null) {
                this.compiledMethod =
                        compileAndLoadMethod(functionName, lambdaExpression, "java.lang.Object");
            }
            List<Object> parameters = new ArrayList<>();
            for (int i = 0; i < inputTypes().length; i++) {
                if (inputTypes()[i] == StringType) {
                    parameters.add(input.getString(i));
                } else if (inputTypes()[i] == IntegerType) {
                    parameters.add(input.getInt(i));
                }
            }
            return UTF8String.fromString(
                    (String)
                            this.compiledMethod.invoke(
                                    null, parameters.toArray(new Object[parameters.size()])));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String name() {
        return functionName;
    }

    @Override
    public String canonicalName() {
        return functionName;
    }

    private Method compileAndLoadMethod(
            String functionName, String lambdaExpression, String returnType) throws Exception {
        String methodSignature = generateMethodSignature(lambdaExpression, returnType);
        String methodBody = generateMethodBody(lambdaExpression);
        String className = "GeneratedLambda" + functionName;
        String fullMethod = methodSignature + " { " + methodBody + " }";

        String javaCode = "public class " + className + " { " + fullMethod + " }";

        File sourceFile = new File(className + ".java");
        try (FileWriter writer = new FileWriter(sourceFile)) {
            writer.write(javaCode);
        }

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        Iterable<? extends JavaFileObject> compilationUnits =
                fileManager.getJavaFileObjectsFromFiles(Arrays.asList(sourceFile));
        compiler.getTask(null, fileManager, null, null, null, compilationUnits).call();
        fileManager.close();
        URLClassLoader classLoader =
                URLClassLoader.newInstance(new URL[] {new File(".").toURI().toURL()});
        Class<?> compiledClass = Class.forName(className, true, classLoader);
        return compiledClass.getDeclaredMethods()[0];
    }

    public static String generateMethodSignature(String lambdaExpression, String returnType) {
        Pattern paramPattern = Pattern.compile("\\((.*?)\\)");
        Matcher paramMatcher = paramPattern.matcher(lambdaExpression);

        if (!paramMatcher.find()) {
            throw new IllegalArgumentException("Invalid lambda expression format");
        }

        String[] params = paramMatcher.group(1).split(",");
        String methodName = "apply";

        StringBuilder signature = new StringBuilder();
        signature
                .append("public static ")
                .append(returnType)
                .append(" ")
                .append(methodName)
                .append("(");

        for (int i = 0; i < params.length; i++) {
            String[] paramParts = params[i].trim().split("\\s+");
            if (paramParts.length != 2) {
                throw new IllegalArgumentException("Invalid parameter format: " + params[i]);
            }
            String paramType = paramParts[1];
            String paramName = paramParts[0];

            signature.append(paramType).append(" ").append(paramName);
            if (i < params.length - 1) {
                signature.append(", ");
            }
        }

        signature.append(")");
        return signature.toString();
    }

    public static String generateMethodBody(String lambdaExpression) {
        String[] parts = lambdaExpression.split("->");
        String body = parts[1].trim();
        if (body.startsWith("{")) {
            return body;
        }
        return "return " + body + ";";
    }
}
