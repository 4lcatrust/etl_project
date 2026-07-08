package jobs

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.json4s._
import org.json4s.native.JsonMethods._
import java.time.LocalDate

/**
 * Generic bronze-extract template: JDBC source -> in-job validation -> Iceberg bronze/quarantine.
 *
 * Port of boreas's df_bronze_pg_cross.py (Beam) onto Spark. Deliberate deviations from the original:
 *  - No ::TEXT cast trick for json/timestamp/numeric columns; Spark's JDBC reader yields proper
 *    Spark SQL types directly, so we CAST to a canonical type instead.
 *  - `logical_check` rules are Spark SQL `expr()` boolean expressions, not a Python eval() sandbox.
 *  - Output is Iceberg tables (bronze/quarantine/audit), not path-partitioned Parquet.
 */
object BronzeExtract {

  // ---------- CLI args ----------
  case class Args(
    single: Map[String, String],
    filterConditions: List[String]
  ) {
    def req(key: String): String =
      single.getOrElse(key, throw new IllegalArgumentException(s"missing required --$key"))
    def opt(key: String): Option[String] = single.get(key)
    def optInt(key: String): Option[Int] = single.get(key).map(_.toInt)
    def optDouble(key: String): Option[Double] = single.get(key).map(_.toDouble)
  }

  def parseArgs(argv: Array[String]): Args = {
    val single = scala.collection.mutable.Map[String, String]()
    val filters = scala.collection.mutable.ListBuffer[String]()
    var i = 0
    while (i < argv.length) {
      val a = argv(i)
      if (a.startsWith("--") && i + 1 < argv.length) {
        val key = a.stripPrefix("--")
        val value = argv(i + 1)
        if (key == "filter_condition") filters += value else single(key) = value
        i += 2
      } else {
        i += 1
      }
    }
    Args(single.toMap, filters.toList)
  }

  // ---------- schema / validation-rule contracts (mirrors the *_vld.yaml shape) ----------
  case class ColumnDef(
    name: String,
    `type`: String,
    alias_name: Option[String] = None,
    expr_override: Option[String] = None,
    nullable: Option[Boolean] = None
  )

  case class ValidationRule(
    id: Option[String] = None,
    rule: String,
    columns: Option[List[String]] = None,
    column: Option[String] = None,
    min: Option[Double] = None,
    max: Option[Double] = None,
    expression: Option[String] = None
  ) {
    def ruleId: String = id.getOrElse(rule)
  }

  implicit val formats: Formats = DefaultFormats

  def parseSchema(json: String): List[ColumnDef] = parse(json).extract[List[ColumnDef]]
  def parseRules(json: String): List[ValidationRule] = parse(json).extract[List[ValidationRule]]

  // ---------- SQL / type-mapping helpers (port of build_select_sql / build_arrow_schema) ----------
  private val decimalRe = """^(?:numeric|decimal)\((\d+)\s*,\s*(\d+)\)$""".r

  /** Canonical Spark SQL type string for a declared column type; used both for the post-read CAST
    * and the Iceberg CREATE TABLE DDL, so the two stay in sync. */
  def sparkTypeFor(colType: String): String = colType.toLowerCase.trim match {
    case decimalRe(p, s)                => s"DECIMAL($p,$s)"
    case "numeric" | "decimal"          => "DECIMAL(38,9)"
    case "string"  | "utf8"             => "STRING"
    case "int64"                        => "BIGINT"
    case "int32"                        => "INT"
    case "float64"                      => "DOUBLE"
    case "float32"                      => "FLOAT"
    case "bool"    | "boolean"          => "BOOLEAN"
    case "bytes"                        => "BINARY"
    case "json"    | "jsonb"            => "STRING"
    case "date"                         => "DATE"
    case "time"                         => "STRING" // no first-class Spark/Iceberg TIME type; keep as text
    case t if t.startsWith("timestamp") => "TIMESTAMP"
    case other =>
      throw new IllegalArgumentException(s"Unknown column type '$other'")
  }

  def isStringType(colType: String): Boolean =
    Set("string", "utf8", "json", "jsonb").contains(colType.toLowerCase.trim)

  /** Build the source SELECT, quoting identifiers and honoring alias_name/expr_override
    * (port of build_select_sql, minus the Beam-only ::TEXT cast trick). `quote` is the
    * source dialect's identifier quote char: `"` for Postgres, `` ` `` for MySQL. */
  def buildSelectSql(schemaName: String, tableName: String, schema: List[ColumnDef],
                     quote: String = "\""): String = {
    def q(id: String): String = s"$quote$id$quote"
    val exprs = schema.map { col =>
      val alias = col.alias_name.getOrElse(col.name)
      col.expr_override match {
        case Some(expr) => s"$expr AS ${q(alias)}"
        case None if alias != col.name => s"${q(col.name)} AS ${q(alias)}"
        case None                      => q(col.name)
      }
    }
    s"SELECT ${exprs.mkString(", ")} FROM $schemaName.${q(tableName)} WHERE 1=1"
  }

  def addFilterConditions(sql: String, conditions: List[String]): String =
    if (conditions.isEmpty) sql else (sql +: conditions).mkString("\n")

  // ---------- validation (port of ValidateDoFn, expressed as Spark columns) ----------
  /** One boolean "violated" column per rule. `logical_check` treats a NULL expr result as a
    * violation (fail-closed), matching boreas's "exception during eval => violation" behavior. */
  def violatedColumn(rule: ValidationRule): Column = rule.rule match {
    case "not_null" =>
      rule.columns.getOrElse(Nil).map(c => col(c).isNull).reduceOption(_ || _).getOrElse(lit(false))
    case "positive_number" =>
      rule.columns.getOrElse(Nil)
        .map(c => col(c).isNotNull && col(c) < 0)
        .reduceOption(_ || _)
        .getOrElse(lit(false))
    case "value_range" =>
      val c = col(rule.column.get)
      val cmp = if (isStringType(rule.column.get)) length(c) else c
      c.isNotNull && !(cmp.between(rule.min.get, rule.max.get))
    case "logical_check" =>
      not(coalesce(expr(rule.expression.get), lit(false)))
    case other =>
      throw new IllegalArgumentException(s"Unknown validation rule type '$other'")
  }

  /** Adds a `violated_rule_ids` array<string> column (empty array = valid). */
  def withViolations(df: DataFrame, rules: List[ValidationRule]): DataFrame = {
    if (rules.isEmpty) return df.withColumn("violated_rule_ids", array().cast("array<string>"))
    val perRule = rules.map(r => when(violatedColumn(r), lit(r.ruleId)).otherwise(lit(null: String)))
    val raw = array(perRule: _*)
    df.withColumn("violated_rule_ids", filter(raw, x => x.isNotNull))
  }

  // Named (not tuple) so Spark's product encoder preserves these as the struct field
  // names rule_id/violation_count — a plain (String, Long) tuple would encode as
  // struct<_1,_2>, which wouldn't match the audit table's declared column type.
  case class RuleViolationCount(rule_id: String, violation_count: Long)

  def makeSpark(appName: String): SparkSession = {
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  def main(argv: Array[String]): Unit = {
    val args = parseArgs(argv)
    val schema = parseSchema(args.req("schema_json"))
    val rules = parseRules(args.req("validation_rules_json"))
    val catalog = args.opt("catalog").getOrElse("iceberg")
    val dbName = args.req("db_name")
    val tableName = args.req("table_name")
    val primaryKey = args.req("primary_key")
    val ingestionDate = args.opt("ingestion_date").getOrElse(LocalDate.now().toString)
    val batchId = sys.env.getOrElse("AIRFLOW_CTX_DAG_RUN_ID", "manual")
    val runTs = sys.env.getOrElse("AIRFLOW_CTX_EXECUTION_DATE", java.time.Instant.now().toString)

    val spark = makeSpark(s"BronzeExtract-$dbName-$tableName")
    import spark.implicits._

    // ---------- 1. read ----------
    // Two source families share this one validation+Iceberg engine: JDBC (Postgres/MySQL) or a
    // JSON file on MinIO (--input_path) that a Python connector landed (REST API, etc.). The
    // rest of the pipeline (cast, validate, split, write, audit) is identical either way.
    val rawDf = args.opt("input_path") match {
      case Some(path) =>
        // NDJSON (one record per line) written by the connector; schema is inferred then
        // coerced to the declared canonical types in step 2, same as the JDBC path.
        println(s"[BronzeExtract] reading JSON source: $path")
        spark.read.json(path)
      case None =>
        // Identifier quote char for the source dialect: `"` (Postgres, default) or `` ` `` (MySQL).
        val identQuote = args.opt("identifier_quote").getOrElse("\"")
        val baseSql = args.opt("query").getOrElse(
          buildSelectSql(args.req("schema_name"), tableName, schema, identQuote)
        )
        val sql = addFilterConditions(baseSql, args.filterConditions)
        println(s"[BronzeExtract] source SQL:\n$sql")

        // JDBC driver class; defaults to Postgres so existing callers are unchanged. MySQL
        // passes --jdbc_driver com.mysql.cj.jdbc.Driver (the same JAR serves both sources).
        val jdbcDriver = args.opt("jdbc_driver").getOrElse("org.postgresql.Driver")
        var reader = spark.read.format("jdbc")
          .option("url", args.req("jdbc_url"))
          .option("query", sql)
          .option("user", args.req("username"))
          .option("password", args.req("password"))
          .option("driver", jdbcDriver)
        args.optInt("fetch_size").foreach(fs => reader = reader.option("fetchsize", fs))
        for {
          numPartitions   <- args.optInt("num_partitions")
          partitionColumn <- args.opt("partition_column")
          lowerBound      <- args.optDouble("lower_bound")
          upperBound      <- args.optDouble("upper_bound")
        } {
          reader = reader
            .option("numPartitions", numPartitions)
            .option("partitionColumn", partitionColumn)
            .option("lowerBound", lowerBound)
            .option("upperBound", upperBound)
        }
        reader.load()
    }

    // ---------- 2. coerce to canonical types ----------
    // Only cast when the JDBC-read type differs from the canonical target. Casting a column to
    // its own type is a no-op that Spark 4.0's SimplifyCasts optimizer mishandles under the
    // collation-aware StringType (PLAN_VALIDATION_FAILED_RULE_IN_BATCH: "previously resolved and
    // now became unresolved"), so we skip it rather than lean on the optimizer to remove it.
    import org.apache.spark.sql.types.DataType
    val rawTypes = rawDf.schema.fields.map(f => f.name -> f.dataType).toMap
    val castExprs = schema.map { c =>
      val alias = c.alias_name.getOrElse(c.name)
      val target = DataType.fromDDL(sparkTypeFor(c.`type`))
      if (rawTypes.get(alias).contains(target)) col(alias)
      else col(alias).cast(target).as(alias)
    }
    val typedDf = rawDf.select(castExprs: _*).withColumn("ingestion_date", lit(ingestionDate).cast("date"))
    val inputCount = typedDf.count()

    // ---------- 3. validate & split ----------
    val flagged = withViolations(typedDf, rules)
    val validDf = flagged.filter(size(col("violated_rule_ids")) === 0).drop("violated_rule_ids")
    val invalidDf = flagged.filter(size(col("violated_rule_ids")) > 0)
    val validCount = validDf.count()
    val invalidCount = invalidDf.count()

    // ---------- 4. write bronze / quarantine (Iceberg, identity-partitioned by ingestion_date) ----------
    val bronzeTable = s"$catalog.bronze.${dbName}__$tableName"
    val quarantineTable = s"$catalog.quarantine.${dbName}__$tableName"
    val auditTable = s"$catalog.audit.validation_summary"

    // Spark SQL quotes identifiers with backticks, not double quotes (double quotes are
    // string literals) -- unlike the Postgres-side buildSelectSql, which correctly uses
    // double quotes since that SQL runs against Postgres.
    val bronzeCols = (schema.map(c => s"`${c.alias_name.getOrElse(c.name)}` ${sparkTypeFor(c.`type`)}") :+
      "ingestion_date DATE").mkString(", ")
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.bronze")
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.quarantine")
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.audit")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $bronzeTable ($bronzeCols) USING iceberg PARTITIONED BY (ingestion_date)")
    spark.sql(s"CREATE TABLE IF NOT EXISTS $quarantineTable ($bronzeCols, violated_rule_ids ARRAY<STRING>) USING iceberg PARTITIONED BY (ingestion_date)")
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $auditTable (
         |  batch_id STRING, db_name STRING, table_name STRING, run_ts STRING,
         |  input_rows BIGINT, valid_rows BIGINT, invalid_rows BIGINT,
         |  violated_rules ARRAY<STRUCT<rule_id: STRING, violation_count: BIGINT>>,
         |  invalid_row_ids ARRAY<STRING>, ingestion_date DATE
         |) USING iceberg PARTITIONED BY (ingestion_date)""".stripMargin
    )

    // Idempotent per ingestion_date: clear this run's partition before appending so a retry
    // or manual re-run of the same date replaces rather than duplicates. The predicate is
    // partition-aligned, so Iceberg resolves it as a metadata-only partition delete.
    val datePred = s"ingestion_date = date'$ingestionDate'"
    spark.sql(s"DELETE FROM $bronzeTable WHERE $datePred")
    spark.sql(s"DELETE FROM $quarantineTable WHERE $datePred")

    validDf.writeTo(bronzeTable).append()
    println(s"[BronzeExtract] Extracted & saved $tableName -> $bronzeTable")
    if (invalidCount > 0) {
      invalidDf.writeTo(quarantineTable).append()
      println(s"[BronzeExtract] Quarantined $invalidCount/$inputCount rows -> $quarantineTable")
    }

    // ---------- 5. audit ----------
    val ruleCounts: Array[Row] =
      if (invalidCount > 0)
        invalidDf.select(explode(col("violated_rule_ids")).as("rule_id"))
          .groupBy("rule_id").count()
          .collect()
      else Array.empty
    val violatedRules = ruleCounts.map(r => RuleViolationCount(r.getString(0), r.getLong(1))).toSeq
    val invalidIds: Seq[String] =
      if (invalidCount > 0)
        invalidDf.select(col(primaryKey).cast("string")).distinct().limit(500)
          .collect().map(_.getString(0)).toSeq
      else Seq.empty

    val auditRow = Seq((
      batchId, dbName, tableName, runTs,
      inputCount, validCount, invalidCount,
      violatedRules, invalidIds, ingestionDate
    )).toDF(
      "batch_id", "db_name", "table_name", "run_ts",
      "input_rows", "valid_rows", "invalid_rows",
      "violated_rules", "invalid_row_ids", "ingestion_date"
    ).withColumn("ingestion_date", col("ingestion_date").cast("date"))

    // Audit is partitioned only by ingestion_date (shared across all tables), so scope the
    // idempotency delete to this db/table/date row-wise -- don't wipe other tables' audit rows.
    spark.sql(s"DELETE FROM $auditTable WHERE db_name = '$dbName' AND table_name = '$tableName' AND $datePred")
    auditRow.writeTo(auditTable).append()
    println(s"[BronzeExtract] Audit: input=$inputCount valid=$validCount invalid=$invalidCount -> $auditTable")

    println("[BronzeExtract] DONE")
    spark.stop()
  }
}
