package org.elevenquest.bgtojdbc;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.elevenquest.bgtojdbc.BigQueryToAvro.BigQueryToAvroOptions;

import com.google.api.services.bigquery.model.TableRow;

/**
Launch Method :
mvn compile exec:java -Dexec.mainClass=org.elevenquest.bgtojdbc.BigQueryToText -Dexec.args="--project=<<project-id>> --tempLocation=gs://<<bigquery resultset templocation>> --output=gs://<<text output gs location>>"
 */

 public class BigQueryToJdbc {

  public interface BigQueryToJdbcOptions extends PipelineOptions {
    /** Set this required option to specify where to write the text output. */
    @Description("Path of the file to write to")
    @Required
    String getOutput();
    void setOutput(String value);

    @Description("jdbc jar location - gs://your/jdbc/jar/location/db.jar")
    @Required
    String getJarLocation();
    void setJarLocation(String value);

    @Description("Database Host to connect on.")
    @Required
    String getDatabaseHost();
    void setDatabaseHost(String value);

    @Description("jdbc driver name - ex: org.postgresql.Driver, com.mysql.cj.jdbc.Driver, com.microsoft.sqlserver.jdbc.SQLServerDriver")
    @Required
    String getJdbcDriverName();
    void setJdbcDriverName(String value);

    @Description("Database identifier - postgres | mysql | sqlserver ")
    @Required
    String getDatabaseIdentifier();
    void setDatabaseIdentifier(String value);

    @Description("Database Port to connect on.")
    @Required
    @Default.String("5432")
    String getDatabasePort();
    void setDatabasePort(String value);

    @Description("Database User to connect with.")
    @Required
    @Default.String("postgres")
    String getDatabaseUser();
    void setDatabaseUser(String value);

    @Description("Database Password for given user.")
    @Required
    String getDatabasePassword();
    void setDatabasePassword(String value);

    @Description("The database name to connect to.")
    @Required
    @Default.String("postgres")
    String getDatabaseName();
    void getDatabaseName(String value);

    @Description("The target table name to insert into")
    @Required
    @Default.String("postgres")
    String getTargetTableName();
    void setTargetTableName(String value);
  }

  static String MYSQL_JDBC_URL_TEMPLATE = "jdbc:mysql://%s:%s/%s"; // host:port/dbname
  static String POSTGRES_JDBC_URL_TEMPLATE = "jdbc:postgres://%s:%s/%s";  // host:port/dbname
  static String SQLSERVER_JDBC_URL_TEMPLATE = "jdbc:sqlserver://%s:%s;database=%s"; // host:port;database=<<databasename>>

  static DataSourceConfiguration getDataSourceConfiguration(BigQueryToJdbcOptions options)
  {
    String jdbcUrl = null;
    String jdbcDriverName = null;
    DataSourceConfiguration ds = null;
    switch (options.getDatabaseIdentifier()){
      case "mysql":
        jdbcUrl = String.format(MYSQL_JDBC_URL_TEMPLATE,options.getDatabaseHost(), options.getDatabasePort(), options.getDatabaseName());
        jdbcDriverName = options.getJdbcDriverName() != null ? options.getJdbcDriverName() : "com.mysql.cj.jdbc.Driver";
        ds = DataSourceConfiguration.create(jdbcDriverName, jdbcUrl);
        break;
      case "postgres":
        jdbcUrl = String.format(POSTGRES_JDBC_URL_TEMPLATE,options.getDatabaseHost(), options.getDatabasePort(), options.getDatabaseName());
        jdbcDriverName = options.getJdbcDriverName() != null ? options.getJdbcDriverName() : "org.postgresql.Driver";
        ds = DataSourceConfiguration.create(jdbcDriverName, jdbcUrl);
        break;
      case "sqlserver":
        jdbcUrl = String.format(POSTGRES_JDBC_URL_TEMPLATE,options.getDatabaseHost(), options.getDatabasePort(), options.getDatabaseName());
        jdbcDriverName = options.getJdbcDriverName() != null ? options.getJdbcDriverName() : "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        ds = DataSourceConfiguration.create(jdbcDriverName, jdbcUrl);
        break;
      default:
        throw new RuntimeException("Unsupported JDBC Driver. Add your jdbc driver configuration rutines in BigQueryToJdbc.getDataSourceConfiguration methods.");
    }
    ds = ds
          .withUsername(options.getDatabaseUser())
          .withPassword(options.getDatabasePassword())
          .withDriverJars(options.getJarLocation());
    return ds;
  }

  static String getInsertStatementFromSchema(Schema schema, BigQueryToJdbcOptions options) {
    StringBuilder fields = new StringBuilder();
    StringBuilder values = new StringBuilder();
      for (Schema.Field field : schema.getFields()) {
        fields.append(",").append(field.getName());
        values.append(",").append("?");
    }
    return String.format("insert into %s (%s) values (%s)", options.getTargetTableName(), fields.substring(1), values.substring(1));
  }

  public static void main(String[] args) {

    BigQueryToJdbcOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToJdbcOptions.class);

    DataSourceConfiguration jdbConfiguration = getDataSourceConfiguration(options);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<TableRow> data = pipeline
      .apply(
        "Read from BigQuery query",
        BigQueryIO.readTableRowsWithSchema().fromQuery("select create_yearmonth, biz_workplace_name, biz_regid, register_status from `looker_dataset.national_pension_mom` limit 10").usingStandardSql()
      );
    Schema schema = data.getSchema();
    data.apply("output to stream",ParDo.of(new DoFn<TableRow, GenericRecord>() {
      @ProcessElement
      public void ProcessElement(@Element TableRow inRow, OutputReceiver<GenericRecord> out) {
        try {
          Row record = BigQueryUtils.toBeamRow(schema, inRow);
          GenericRecord genericRecord = AvroUtils.toGenericRecord(record);
          out.output(genericRecord);
        } catch(Exception e)
        {
          e.printStackTrace();
        }
      }
    }))
    // Without below lines. you might see this message - java.lang.IllegalStateException: Unable to return a default Coder for output to stream/ParMultiDo(Anonymous).output [PCollection@644906303]
    .setCoder(AvroUtils.schemaCoder(AvroUtils.toAvroSchema(schema)))
    .apply("output to system console", AvroIO.writeGenericRecords(AvroUtils.toAvroSchema(schema))
      .to(options.getOutput())
      .withSuffix(".avro")
    );

    pipeline.run();
  }
}
