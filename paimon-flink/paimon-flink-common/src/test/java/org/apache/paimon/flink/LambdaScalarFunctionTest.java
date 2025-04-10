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

import org.junit.Test;

import static org.junit.Assert.*;

public class LambdaScalarFunctionTest {

    @Test
    public void testSimpleAddition() throws Exception {
        String lambda = "(x Integer, y Integer) -> { x = x + 1; return x + y; }";
        LambdaScalarFunction function =
                new LambdaScalarFunction(
                        "add", new String[] {"Integer", "Integer"}, lambda, "Integer");
        function.open(null);
        assertEquals(6, function.eval(2, 3));
    }

    @Test
    public void testStringConcatenation() throws Exception {
        String lambda = "(firstName String, lastName String) -> firstName + \" \" + lastName";
        LambdaScalarFunction function =
                new LambdaScalarFunction(
                        "testString", new String[] {"String", "String"}, lambda, "String");
        function.open(null);
        assertEquals("John Doe", function.eval("John", "Doe"));
    }

    @Test
    public void testComplexExpression() throws Exception {
        String lambda =
                "(x Double, y Double, z Double) -> Math.pow(x, 2) + Math.pow(y, 2) + Math.pow(z, 2)";
        LambdaScalarFunction function =
                new LambdaScalarFunction(
                        "testPow", new String[] {"Double", "Double", "Double"}, lambda, "Double");
        function.open(null);
        double result = (double) function.eval(3.0, 4.0, 5.0);
        assertEquals(50.0, result, 0.0001);
    }
}
