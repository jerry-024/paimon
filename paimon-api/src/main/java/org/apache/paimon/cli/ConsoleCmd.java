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

import org.apache.paimon.cli.utils.CliParams;
import org.apache.paimon.cli.utils.Exit;
import org.apache.paimon.cli.utils.Printer;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.utils.StringUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** ConsoleCmd. */
public class ConsoleCmd {

    private static Properties properties = new Properties();

    public static void main(String[] args) {
        RESTApi api = createRestApi();
        execute(api, args);
    }

    public static void execute(RESTApi api, String[] args) {
        Printer.log(String.format("exec %s", Printer.toStringify(args)));
        try {
            String object = "table";
            String action = "list";
            if (args.length > 1) {
                object = args[0];
                action = args[1];
            } else {
                Printer.printException(
                        "missing object or action so use the default value: table list.", null);
            }
            String[] params = {};
            if (args.length > 2) {
                params = Arrays.copyOfRange(args, 2, args.length);
            }
            org.apache.commons.cli.Options cliOptions = new org.apache.commons.cli.Options();
            Arrays.stream(CliParams.values())
                    .forEach(
                            cliParam ->
                                    cliOptions.addOption(
                                            Option.builder()
                                                    .longOpt(cliParam.val())
                                                    .hasArg()
                                                    .build()));
            CommandLineParser parser = new DefaultParser();
            // Parse the command line arguments
            CommandLine cmd = parser.parse(cliOptions, params);
            Map<String, String> cliParamsMap = createMapFromOptions(cmd);
            switch (object) {
                case "table":
                    switch (action) {
                        case "list":
                            String databaseName = cliParamsMap.get(CliParams.DATABASE_NAME.val());
                            Printer.log(api.listTables(databaseName));
                            break;
                    }
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            Exit.error("failed.....", e);
        }
    }

    public static Map<String, String> createMapFromOptions(CommandLine cmd) {
        Map<String, String> map = new HashMap<>();
        for (Option opt : cmd.getOptions()) {
            String optValue = opt.getValue();
            if (optValue != null) {
                map.put(opt.getLongOpt(), optValue);
            }
        }
        return map;
    }

    private static RESTApi createRestApi() {
        String paimonCliConfigDir = "./conf";
        if (StringUtils.isNotEmpty(System.getenv("PAIMON_HOME"))) {
            paimonCliConfigDir = System.getenv("PAIMON_HOME") + "/conf";
        }
        try {
            Map<String, String> conf =
                    loadProperties(paimonCliConfigDir + "/paimon-cli.properties");
            Options options = new Options(conf);
            return new RESTApi(options);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Could not find config file", e);
        }
    }

    private static Map<String, String> loadProperties(String filePath)
            throws FileNotFoundException {
        try (InputStream input = new FileInputStream(filePath)) {
            properties.load(input);
            Map<String, String> map = new HashMap<>();
            for (String key : properties.stringPropertyNames()) {
                String value = properties.getProperty(key);
                map.put(key, value);
            }
            return map;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load YAML file: " + filePath, e);
        }
    }
}
