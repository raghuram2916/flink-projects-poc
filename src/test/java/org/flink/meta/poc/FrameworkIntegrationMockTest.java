package org.flink.meta.poc;

import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;

import java.util.List;
import org.flink.meta.poc.udfs.JsonFieldExtractorUdf;

public class FrameworkIntegrationMockTest {

  public static void main(String[] args) throws Exception {
    TableEnvironment tableEnv = TableEnvironment.create(
        EnvironmentSettings.newInstance().inStreamingMode().build()
    );

    System.out.println("TEST ---");
    // Register your UDF
    tableEnv.createTemporarySystemFunction("JsonFieldExtractorUdf", JsonFieldExtractorUdf.class);

    // Create mock data
    List<Row> userRows = List.of(
        Row.of("{\"_id\":{\"$oid\":\"UNIQUE123\"},\"username\":\"john_doe\"}"),
        Row.of("{\"_id\":{\"$oid\":\"UNIQUE223\"},\"username\":\"jane_smith\"}")
    );

    List<Row> salesRows = List.of(
        Row.of("{\"userId\":\"UNIQUE123\",\"totalSpent\":{\"$numberDecimal\":\"250.75\"},\"sale_date\":{\"$date\":\"2025-07-06T10:00:00Z\"},\"promo_code\":\"SUMMER25\"}"),
        Row.of("{\"userId\":\"UNIQUE223\",\"totalSpent\":{\"$numberDecimal\":\"100.00\"},\"sale_date\":{\"$date\":\"2025-07-06T09:00:00Z\"},\"promo_code\":\"WELCOME\"}")
    );

    List<Row> promoRows = List.of(
        Row.of("{\"promo_code\":\"SUMMER25\",\"campaign_name\":\"Summer Sale\",\"start_date\":{\"$date\":\"2025-07-01T00:00:00Z\"},\"end_date\":{\"$date\":\"2025-07-31T23:59:59Z\"}}"),
        Row.of("{\"promo_code\":\"WELCOME\",\"campaign_name\":\"Welcome Offer\",\"start_date\":{\"$date\":\"2025-06-01T00:00:00Z\"},\"end_date\":{\"$date\":\"2025-07-10T23:59:59Z\"}}")
    );

    // Register in-memory tables
    tableEnv.createTemporaryView("user_source", tableEnv.fromValues(
        DataTypes.ROW(DataTypes.FIELD("data", DataTypes.STRING())), userRows));

    tableEnv.createTemporaryView("sales_source", tableEnv.fromValues(
        DataTypes.ROW(DataTypes.FIELD("data", DataTypes.STRING())), salesRows));

    tableEnv.createTemporaryView("promo_source", tableEnv.fromValues(
        DataTypes.ROW(DataTypes.FIELD("data", DataTypes.STRING())), promoRows));

    // SQL query
    String query = """
            SELECT
              CAST(JsonFieldExtractorUdf(user_source.data, '_id.$oid') AS STRING) AS id,
              CAST(JsonFieldExtractorUdf(sales_source.data, 'userId') AS STRING) AS userId,
              CAST(JsonFieldExtractorUdf(user_source.data, 'username') AS STRING) AS username,
              CAST(CAST(JsonFieldExtractorUdf(sales_source.data, 'totalSpent.$numberDecimal') AS DECIMAL(10,2)) AS DECIMAL(10, 2)) AS totalSpent,
              CAST(CAST(JsonFieldExtractorUdf(sales_source.data, 'sale_date.$date') AS TIMESTAMP_LTZ(3)) AS TIMESTAMP_LTZ(3)) AS sale_date,
              CAST(JsonFieldExtractorUdf(sales_source.data, 'promo_code') AS STRING) AS promo_code,
              CAST(COALESCE(JsonFieldExtractorUdf(promo_source.data, 'campaign_name'), 'N/A') AS STRING) AS campaign_name,
              CAST(CAST(JsonFieldExtractorUdf(promo_source.data, 'start_date.$date') AS TIMESTAMP_LTZ(3)) AS TIMESTAMP_LTZ(3)) AS start_date,
              CAST(CAST(JsonFieldExtractorUdf(promo_source.data, 'end_date.$date') AS TIMESTAMP_LTZ(3)) AS TIMESTAMP_LTZ(3)) AS end_date,
              DATE '2025-07-06' AS processing_date
            FROM user_source
            LEFT JOIN sales_source ON CAST(JsonFieldExtractorUdf(user_source.data, '_id.$oid') AS STRING) = CAST(JsonFieldExtractorUdf(sales_source.data, 'userId') AS STRING)
            LEFT JOIN promo_source ON CAST(JsonFieldExtractorUdf(sales_source.data, 'promo_code') AS STRING) = CAST(JsonFieldExtractorUdf(promo_source.data, 'promo_code') AS STRING) 
              AND CAST(JsonFieldExtractorUdf(sales_source.data, 'sale_date.$date') AS TIMESTAMP_LTZ(3)) BETWEEN CAST(JsonFieldExtractorUdf(promo_source.data, 'start_date.$date') AS TIMESTAMP_LTZ(3)) AND CAST(JsonFieldExtractorUdf(promo_source.data, 'end_date.$date') AS TIMESTAMP_LTZ(3))
        """;

    tableEnv.sqlQuery(query).execute().print();
  }
}