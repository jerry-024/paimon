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

package org.apache.paimon.cli.utils;

/** Exit. */
public class Exit {

    public static void error(final String errorMessage, final Exception e) {
        Printer.printException(errorMessage, e);
        System.exit(1);
    }

    public static void error(final String error) {
        error(error, null);
    }

    public static void error(final Exception e) {
        error(null, e);
    }

    public static void error() {
        System.exit(1);
    }

    public static void success(String successMessage) {
        if (successMessage != null) {
            Printer.println(successMessage);
        } else {
            System.exit(0);
        }
    }

    public static void success() {
        success(null);
    }
}
