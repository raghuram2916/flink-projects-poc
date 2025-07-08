package org.flink.meta.poc;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.flink.meta.poc.config.IngestionConfig;
import org.flink.meta.poc.udfs.JsonFieldExtractorUdf;
import org.flink.meta.poc.util.ConfigLoader;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class FrameworkIntegrationTest {

  @Test
  void testYamlLoading() {
    IngestionConfig config = ConfigLoader.load("test-ingestion-config.yaml");
    //InputStream in = this.getClass().getClassLoader().getResourceAsStream("test-ingestion-config.yaml");
    //System.out.println(new BufferedReader(new InputStreamReader(in)).lines().collect(Collectors.joining("\n")));

    assertNotNull(config.sources);
    System.out.println("Sources: " + config.sources.size());
  }


  //@Test
  void testEndToEndPipelineWithMockData() throws Exception {
    // 1. Load test configuration
    IngestionConfig config = ConfigLoader.load("test-ingestion-config.yaml");
    System.out.println("Loaded config: " + new ObjectMapper().writeValueAsString(config));

    DDLGenerator.loadRuleTemplates(config);

    // 2. Setup Flink Test Environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    // 3. Register UDFs
    tEnv.createTemporarySystemFunction("JsonFieldExtractorUdf", JsonFieldExtractorUdf.class);

    // 4. Prepare Mock/In-Memory Data
    // User with a valid sale and promotion
    String userData = "{\"_id\": {\"$oid\": \"user1\"}, \"username\": \"jdoe\"}";
    String saleData = "{\"_id\": {\"$oid\": \"sale1\"}, \"userId\": \"user1\", \"totalSpent\": {\"$numberDecimal\": \"150.75\"}, \"sale_date\": {\"$date\": \"2025-07-01T10:00:00Z\"}, \"promo_code\": \"SUMMER25\"}";
    String promoData = "{\"promo_code\": \"SUMMER25\", \"campaign_name\": \"Summer Sale\", \"start_date\": {\"$date\": \"2025-06-01T00:00:00Z\"}, \"end_date\": {\"$date\": \"2025-08-01T00:00:00Z\"}}";

    // User with a sale but no matching promotion (wrong code)
    String userData2 = "{\"_id\": {\"$oid\": \"user2\"}, \"username\": \"jane\"}";
    String saleData2 = "{\"_id\": {\"$oid\": \"sale2\"}, \"userId\": \"user2\", \"totalSpent\": {\"$numberDecimal\": \"50.00\"}, \"sale_date\": {\"$date\": \"2025-07-02T12:00:00Z\"}, \"promo_code\": \"WINTER25\"}";


    // Register data collections that the 'values' connector will use via 'data-id'
    tEnv.createTemporaryView("in_memory_user_data", tEnv.fromValues(Row.of(userData), Row.of(userData2)));
    tEnv.createTemporaryView("in_memory_sales_data", tEnv.fromValues(Row.of(saleData), Row.of(saleData2)));
    tEnv.createTemporaryView("in_memory_promo_data", tEnv.fromValues(Row.of(promoData)));


    // 5. Generate and Execute DDLs for the 'values' sources
    for (String ddl : DDLGenerator.generateAllSourceDDLs(config)) {
      tEnv.executeSql(ddl);
    }

    // 6. Generate the main SELECT query (without the INSERT INTO part)
    String selectQuery = DDLGenerator.generateSelectClause(config) + " " + DDLGenerator.generateFromAndJoinClause(config);
    System.out.println("Executing Query:\n" + selectQuery);
    TableResult result = tEnv.executeSql(selectQuery);

    // 7. Collect results and assert correctness
    List<Row> resultRows = new ArrayList<>();
    try (CloseableIterator<Row> it = result.collect()) {
      while(it.hasNext()){
        resultRows.add(it.next());
      }
    }

    assertThat(resultRows).hasSize(2);

    // Assert first row (successful join with promotion)
    Row row1 = resultRows.stream().filter(r -> r.getField("id").equals("user1")).findFirst().get();
    assertThat(row1.getField("username")).isEqualTo("jdoe");
    assertThat(row1.getField("campaign_name")).isEqualTo("Summer Sale");
    assertThat(row1.getField("totalSpent").toString()).isEqualTo("150.75");

    // Assert second row (left join, promotion fields should be null, then defaulted)
    Row row2 = resultRows.stream().filter(r -> r.getField("id").equals("user2")).findFirst().get();
    assertThat(row2.getField("username")).isEqualTo("jane");
    assertThat(row2.getField("campaign_name")).isEqualTo("N/A"); // Check the default value
    assertThat(row2.getField("totalSpent").toString()).isEqualTo("50.00");
  }
}