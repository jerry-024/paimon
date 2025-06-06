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

/**
 * Enum for CLI parameters.
 *
 * <p>The enum values are used as the key for the command line arguments.
 */
public enum CliParams {
    DATABASE_NAME("database", "The name of the database."),
    VERSION("version", "The version of the API."),
    ;
    private final String value;
    private final String helpMessage;

    CliParams(String value) {
        this.value = value;
        this.helpMessage = "";
    }

    CliParams(String value, String helpMessage) {
        this.value = value;
        this.helpMessage = helpMessage;
    }

    public String val() {
        return value;
    }

    public String getHelpMessage() {
        return helpMessage;
    }

    public static CliParams fromString(String text) {
        for (CliParams cliParam : CliParams.values()) {
            if (cliParam.value.equalsIgnoreCase(text)) {
                return cliParam;
            }
        }
        throw new IllegalArgumentException("No enum constant for value: " + text);
    }

    public static boolean contains(String text) {
        for (CliParams cliParam : CliParams.values()) {
            if (cliParam.value.equalsIgnoreCase(text)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return value;
    }
}
