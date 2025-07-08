package org.flink.meta.poc;

import org.flink.meta.poc.config.IngestionConfig;
import org.flink.meta.poc.util.ConfigLoader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DDLGeneratorTest {

  private static IngestionConfig config;

  @BeforeAll
  static void setUp() {
    // Load the test-specific config once for all tests
    config = ConfigLoader.load("test-ingestion-config.yaml");
    DDLGenerator.loadRuleTemplates(config);
  }

  //@Test
  void testGenerateETLStatementWithComplexJoin() {
    // --- THIS IS THE CORRECTED TEST ---
    // We test the public method that generates the entire ETL statement.
    String etlSql = DDLGenerator.generateETLStatement(config);

    // Print the generated SQL for visual inspection during test runs
    System.out.println("Generated ETL SQL for Test:\n" + etlSql);

    // Assert that the generated string contains the correctly resolved join condition
    assertThat(etlSql)
        .contains("LEFT JOIN promo_source ON CAST(JsonFieldExtractorUdf(sales_source.data, 'promo_code') AS STRING) = CAST(JsonFieldExtractorUdf(promo_source.data, 'promo_code') AS STRING) " +
            "AND CAST(JsonFieldExtractorUdf(sales_source.data, 'sale_date['$date']') AS TIMESTAMP_LTZ(3)) " +
            "BETWEEN CAST(JsonFieldExtractorUdf(promo_source.data, 'start_date['$date']') AS TIMESTAMP_LTZ(3)) " +
            "AND CAST(JsonFieldExtractorUdf(promo_source.data, 'end_date['$date']') AS TIMESTAMP_LTZ(3))");
  }

  //@Test
  void testGenerateFullETLStatementStructure() {
    String etlSql = DDLGenerator.generateETLStatement(config);

    assertThat(etlSql).startsWith("INSERT INTO test_output SELECT");
    assertThat(etlSql).contains("FROM user_source");
    assertThat(etlSql).contains("LEFT JOIN sales_source ON");
    assertThat(etlSql).contains("LEFT JOIN promo_source ON");
    assertThat(etlSql).contains("AS campaign_name");
    // Assert that the default value rule is correctly applied
    assertThat(etlSql).contains("COALESCE(CAST(JsonFieldExtractorUdf(promo_source.data, 'campaign_name') AS STRING), 'N/A')");
  }
}