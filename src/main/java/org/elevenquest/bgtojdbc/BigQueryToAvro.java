package org.elevenquest.bgtojdbc;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import com.google.api.services.bigquery.model.TableRow;

/**
Launch Method :
mvn compile exec:java -Dexec.mainClass=org.elevenquest.bgtojdbc.BigQueryToText -Dexec.args="--project=<<project-id>> --tempLocation=gs://<<bigquery resultset templocation>> --output=gs://<<text output gs location>>"
 */

 public class BigQueryToAvro {

  public interface BigQueryToAvroOptions extends PipelineOptions {
    /** Set this required option to specify where to write the text output. */
    @Description("Path of the file to write to")
    @Required
    String getOutput();
    void setOutput(String value);
  }

  public static void main(String[] args) {

    BigQueryToAvroOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToAvroOptions.class);
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
