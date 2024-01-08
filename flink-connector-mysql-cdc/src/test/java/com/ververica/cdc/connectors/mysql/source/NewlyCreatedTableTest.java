/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.testutils.UniqueDatabase;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

/** IT tests for {@link MySqlSource}. */
public class NewlyCreatedTableTest extends MySqlSourceTestBase {

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);

    private final UniqueDatabase testDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "test", "mysqluser", "mysqlpw");

    private String getServerId() {
        final Random random = new Random();
        int serverId = random.nextInt(100) + 5400;
        return serverId + "-" + (serverId + DEFAULT_PARALLELISM);
    }

    @Test
    public void testNewlyCreatedTables() throws Exception {
        // initialize MySQL
        testDatabase.createAndInitialize();

        // build source
        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .username(testDatabase.getUsername())
                        .password(testDatabase.getPassword())
                        .serverTimeZone("UTC")
                        .serverId(getServerId())
                        .splitSize(8096)
                        .splitMetaGroupSize(1000)
                        .distributionFactorUpper(1000.0d)
                        .distributionFactorLower(0.05d)
                        .fetchSize(1024)
                        .connectTimeout(Duration.ofSeconds(30))
                        .connectMaxRetries(3)
                        .connectionPoolSize(20)
                        .debeziumProperties(new Properties())
                        .startupOptions(StartupOptions.initial())
                        .jdbcProperties(new Properties())
                        .heartbeatInterval(Duration.ofSeconds(30))
                        // to test newly created table DDL
                        .includeSchemaChanges(true)
                        // accept all tables
                        .databaseList(testDatabase.getDatabaseName())
                        .tableList(testDatabase.getDatabaseName() + ".*")
                        // to print dbz json data
                        .deserializer(
                                new JsonDebeziumDeserializationSchema(true, Collections.emptyMap()))
                        // check this feature
                        .scanNewlyAddedTableEnabled(true)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(2000L);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStreamSource<String> source =
                env.fromSource(
                        mySqlSource, WatermarkStrategy.noWatermarks(), "MySql String Source");

        // print JSON
        source.flatMap(
                        (FlatMapFunction<String, String>)
                                (s, collector) -> {
                                    System.out.println(s);
                                    collector.collect(s);
                                })
                .returns(BasicTypeInfo.STRING_TYPE_INFO)
                .addSink(new DiscardingSink<>());

        env.executeAsync();

        Thread.sleep(5_000);

        try (Connection connection = testDatabase.getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("USE " + testDatabase.getDatabaseName());

            // insert value into existed t0
            statement.executeUpdate("INSERT INTO t0 VALUES (3, 'C')");

            // create table t1
            statement.executeUpdate("CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(10))");
            // insert value into new table t1
            statement.executeUpdate("INSERT INTO t1 VALUES (1, 'A')");
        }

        while (true) {
            // check output from FlatMapFunction
            // expected totally 5 outputs
        }
    }
}
