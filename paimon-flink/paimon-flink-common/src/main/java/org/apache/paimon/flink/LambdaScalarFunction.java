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

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;

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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LambdaScalarFunction extends ScalarFunction
        implements FunctionDefinition, Serializable {

    private static final long serialVersionUID = 1L;

    private DataType[] inputTypes;
    private String[] inputTypesString;
    private DataType resultType;
    private String resultTypeString;
    private String functionName;
    private String lambdaExpression;
    private transient Method compiledMethod;

    public LambdaScalarFunction(
            String functionName,
            String[] inputTypesString,
            String lambdaExpression,
            String returnType) {
        try {
            this.inputTypesString = inputTypesString;
            this.resultTypeString = returnType;
            this.functionName = functionName;
            this.lambdaExpression = lambdaExpression;
        } catch (Exception e) {
            throw new RuntimeException("Failed to compile lambda expression", e);
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.compiledMethod =
                compileAndLoadMethod(
                        this.functionName, this.lambdaExpression, this.resultTypeString);
    }

    String getClassName() {
        return LambdaScalarFunction.class.getName();
    }

    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        TypeInference.Builder builder = TypeInference.newBuilder();

        if (inputTypesString != null) {
            List<DataType> inputDataTypes = new ArrayList<>(inputTypesString.length);
            for (String inputType : inputTypesString) {
                inputDataTypes.add(typeFactory.createDataType(inputType));
            }
            inputTypes = inputDataTypes.toArray(new DataType[0]);
        }

        if (inputTypes != null) {
            final List<DataType> argumentDataTypes =
                    Stream.of(inputTypes).collect(Collectors.toList());
            builder.typedArguments(argumentDataTypes);
        }

        if (resultType == null) {
            resultType = typeFactory.createDataType(resultTypeString);
        }

        return builder.outputTypeStrategy(TypeStrategies.explicit(resultType)).build();
    }

    public Object eval(Object... args) {
        try {
            return compiledMethod.invoke(null, args);
        } catch (Exception e) {
            throw new RuntimeException("Failed to evaluate lambda expression", e);
        }
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
}
