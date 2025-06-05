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

package org.apache.paimon.cli;

import org.apache.paimon.cli.utils.Exit;
import org.apache.paimon.cli.utils.Printer;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_ID;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_SECRET;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_REGION;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;
import static org.apache.paimon.rest.RESTCatalogOptions.URI;

/** ConsoleCmd. */
public class ConsoleCmd {

    public static void main(String[] args) {
        ConsoleCmd.installSignal();
        RESTApi api = createRestApi();
        execute(api, args);
    }

    public static void execute(RESTApi api, String[] args) {
        try {
            String object = "table";
            String action = "list";
            if (args.length > 2) {
                object = args[0];
                action = args[1];
            }
            Printer.log(String.format("%s %s", object, action));
            Printer.log(api.listTables("default"));
        } catch (Exception e) {
            Exit.error("failed.....", e);
        }
    }

    private static RESTApi createRestApi() {
        Options options = new Options();
        options.set(URI, "http://dlf-regres-test-cn-hangzhou-vpc.taobao.net");
        options.set(DLF_REGION, "cn-hangzhou");
        options.set(WAREHOUSE, "bennett_e2e_test");
        options.set(TOKEN_PROVIDER, "dlf");
        options.set(DLF_ACCESS_KEY_ID, "LTAI5tFwPB5jjvfkHXYvj5Gt");
        options.set(DLF_ACCESS_KEY_SECRET, "xxxx");
        return new RESTApi(options);
    }

    private static void installSignal() {
        SignalHandler signalHandler = signal -> Exit.error("Stopped successfully.");
        Signal.handle(new Signal("INT"), signalHandler);
        Signal.handle(new Signal("HUP"), signalHandler);
        Signal.handle(new Signal("TERM"), signalHandler);
    }
}
