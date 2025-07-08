package org.flink.meta.poc;

import org.flink.meta.poc.config.IngestionConfig;
import org.flink.meta.poc.config.JoinConfig;
import org.flink.meta.poc.config.SchemaField;
import org.flink.meta.poc.config.SourceConfig;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DDLGenerator {

  private static final Pattern ALIAS_FIELD_PATTERN = Pattern.compile("([a-zA-Z0-9_]+)\\.([a-zA-Z0-9_]+)");

  private static Map<String, String> ruleTemplates = Collections.emptyMap();

  public static void loadRuleTemplates(IngestionConfig config) {
    ruleTemplates = config.ruleTemplates != null ? config.ruleTemplates : Collections.emptyMap();
  }

  public static List<String> generateAllSourceDDLs(IngestionConfig config) {
    List<String> ddls = new ArrayList<>();
    for (SourceConfig source : config.sources) {
      String ddl;
      if ("datagen".equalsIgnoreCase(source.connector)) {
        // Example for datagen source with one key field of STRING type and a data field
        ddl = String.format("""
          CREATE TABLE %s_source (
            `%s` STRING,
            `data` STRING
          ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '10',
            'fields.%s.kind' = 'sequence',
            'fields.%s.start' = '1',
            'fields.%s.end' = '1000',
            'bounded' = 'true'
          );
          """,
            source.alias,
            source.keyField,
            source.keyField,
            source.keyField,
            source.keyField);
      }else {
        ddl = String.format("""
                    CREATE TABLE %s_source (
                      `%s` STRING,
                      `data` STRING,
                      PRIMARY KEY (`%s`) NOT ENFORCED
                    ) WITH (
                      'connector' = '%s',
                      'hosts' = 'localhost:27017',
                      'database' = 'testdb',
                      'collection' = '%s',
                      'scan.startup.mode' = 'initial',
                      'format' = 'json',
                      'json.include.primary.key' = 'true'
                    );
                  """,
            source.alias,
            source.keyField,
            source.keyField,
            source.connector,
            source.collection);
      }
      ddls.add(ddl);
    }
    return ddls;
  }

  public static String generateETLStatement(IngestionConfig config) {
    String selectClause = generateSelectClause(config);
    String fromAndJoinClause = generateFromAndJoinClause(config);
    return "INSERT INTO " + config.target.table + " " + selectClause + " " + fromAndJoinClause;
  }

  public static String generateSelectClause(IngestionConfig config) {
    Map<String, List<String>> typeRules = config.typeRules != null
        ? config.typeRules
        : Collections.emptyMap();

    String selections = config.schema.stream().map(field -> {
      String fieldExpression;
      if (field.sourceAlias == null) {
        fieldExpression = field.sourceField;
      } else {
        String normalizedPath = normalizeJsonPath(field.sourceField);
        String baseExpr = String.format("JsonFieldExtractorUdf(%s_source.data, '%s')", field.sourceAlias, normalizedPath);
        fieldExpression = applyRules(baseExpr, field, typeRules);
      }
      return "  " + fieldExpression + " AS " + field.name;
    }).collect(Collectors.joining(",\n"));

    return "SELECT\n" + selections;
  }

  private static String applyRules(String initialExpr, SchemaField field, Map<String, List<String>> typeRules) {
    if (typeRules == null) {
      typeRules = Collections.emptyMap();
    }

    String expr = initialExpr;

    List<String> rulesToApply = field.rules != null
        ? field.rules
        : (typeRules != null ? typeRules.getOrDefault(field.type, Collections.emptyList()) : Collections.emptyList());

    for (String rule : rulesToApply) {
      expr = resolveRule(rule, expr);
    }

    if (field.defaultValue != null) {
      expr = String.format("COALESCE(%s, '%s')", expr, field.defaultValue);
    }

    return String.format("CAST(%s AS %s)", expr, field.type);
  }

  // === PATCHED method to fix TO_TIMESTAMP_LTZ error ===
  private static String resolveRule(String rule, String fieldExpr) {
    if ("toTimestampLtz".equals(rule)) {
      // Fix: Instead of TO_TIMESTAMP_LTZ on STRING, do CAST(fieldExpr AS TIMESTAMP_LTZ(3))
      return String.format("CAST(%s AS TIMESTAMP_LTZ(3))", fieldExpr);
    }

    String template = ruleTemplates.getOrDefault(rule, rule);
    return template.replace("${field}", fieldExpr);
  }
  // ===================================================

  public static String generateFromAndJoinClause(IngestionConfig config) {
    if (config.joins == null || config.joins.isEmpty()) {
      return "FROM " + config.sources.get(0).alias + "_source";
    }

    StringBuilder fromClause = new StringBuilder();
    fromClause.append("FROM ").append(config.joins.get(0).left).append("_source\n");

    for (JoinConfig join : config.joins) {
      String updatedCondition = generateResolvedJoinCondition(join.condition, config);

      fromClause.append("  ").append(join.type.toUpperCase()).append(" JOIN ")
          .append(join.right).append("_source ON ")
          .append(updatedCondition).append("\n");
    }
    return fromClause.toString();
  }

  private static String generateResolvedJoinCondition(String originalCondition, IngestionConfig config) {
    Matcher matcher = ALIAS_FIELD_PATTERN.matcher(originalCondition);
    StringBuffer finalCondition = new StringBuffer();

    while (matcher.find()) {
      String alias = matcher.group(1);
      String logicalFieldName = matcher.group(2);

      SchemaField field = findSchemaField(config, alias, logicalFieldName);
      String normalizedPath = normalizeJsonPath(field.sourceField);

      String replacement = String.format("CAST(JsonFieldExtractorUdf(%s_source.data, '%s') AS %s)",
          alias, normalizedPath, field.type);

      matcher.appendReplacement(finalCondition, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(finalCondition);

    return finalCondition.toString();
  }

  private static SchemaField findSchemaField(IngestionConfig config, String alias, String logicalFieldName) {
    return config.schema.stream()
        .filter(f -> f.name.equals(logicalFieldName) && alias.equals(f.sourceAlias))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException(
            "Could not find schema definition for join key: " + alias + "." + logicalFieldName +
                ". Available fields: " + config.schema.stream()
                .filter(f -> alias.equals(f.sourceAlias))
                .map(f -> f.name)
                .collect(Collectors.toList())));
  }

  public static String generateTargetDDL(IngestionConfig config) {
    String columns = config.schema.stream()
        .map(f -> String.format("  `%s` %s", f.name, f.type))
        .collect(Collectors.joining(",\n"));

    String partitions = String.join(", ", config.target.partitionedBy);

    return String.format("""
            CREATE TABLE %s (
            %s
            ) PARTITIONED BY (%s)
            WITH (
              'connector' = 'iceberg',
              'catalog-name' = 'iceberg_catalog',
              'warehouse' = 'file:///tmp/iceberg_warehouse/',
              'format-version' = '2'
            );
            """, config.target.table, columns, partitions);
  }

  // 🔧 Helper to normalize JSONPath-like strings into dotted notation
  private static String normalizeJsonPath(String rawPath) {
    if (rawPath == null) return null;
    return rawPath
        .replaceAll("\\[\"(\\$?[^\"]+)\"\\]", ".$1")
        .replaceAll("\\['(\\$?[^']+)'\\]", ".$1")
        .replaceAll("^\\.", "");
  }
}
