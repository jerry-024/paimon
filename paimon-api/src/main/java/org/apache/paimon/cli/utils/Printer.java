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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.SerializationFeature;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.paimon.rest.RESTApi.OBJECT_MAPPER;

/** Printer. */
public class Printer {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static boolean isOutputJSON() {
        return true;
    }

    public static String toStringify(final Object object) {
        if (null == object) {
            return null;
        }

        if (Printer.isOutputJSON()) {
            try {
                ObjectMapper objectMapper = OBJECT_MAPPER;
                objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
                return objectMapper.writeValueAsString(object);
            } catch (JsonProcessingException ignore) {
            }
        }
        return object.toString();
    }

    public static void log(final Object object) {
        Printer.log(toStringify(object));
    }

    public static void log(final String msg) {
        println(String.format("%s : %s", sdf.format(new Date()), msg));
    }

    public static void println(final Object object) {
        Printer.println(toStringify(object));
    }

    public static void println(final String msg) {
        System.out.println(msg);
        System.out.flush();
    }

    public static void printException(final String msg, final Exception e) {
        System.err.println(msg);
        if (null != e) {
            e.printStackTrace();
        }
        System.out.flush();
    }
}
