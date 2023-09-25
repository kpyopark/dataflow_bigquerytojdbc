package org.elevenquest.bgtojdbc;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableRow;

/**
Launch Method :
mvn compile exec:java -Dexec.mainClass=org.elevenquest.bgtojdbc.BigQueryToText -Dexec.args="--project=<<project-id>> --tempLocation=gs://<<bigquery resultset templocation>> --output=gs://<<text output gs location>>"
 */

 public class BigQueryToText {
//   public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
//     @Override
//     public String apply(KV<String, Long> input) {
//       return input.getKey() + ": " + input.getValue();
//     }
//   }

//   public static org.apache.avro.Schema convertBeamSchemaToAvroSchema(Schema beamSchema) {
//     org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().record("beam_record").fields();

//     // Iterate through the fields in the Beam schema
//     for (Schema.Field field : beamSchema.getFields()) {
//         // Determine the Avro type based on the Beam type
//         org.apache.avro.Schema avroFieldType = convertBeamTypeToAvroType(field.get ());

//         // Create an Avro field and add it to the Avro schema
//         Field avroField = new Field(field.getName(), avroFieldType, null, null);
//         avroSchema = avroSchema.name("beam_record").fields().name(field.getName()).type(avroFieldType).noDefault().endRecord();
//     }

//     return avroSchema;
// }

// private static org.apache.avro.Schema convertBeamTypeToAvroType(Schema.FieldType beamType) {
//     switch (beamType.getTypeName()) {
//         case STRING:
//             return org.apache.avro.Schema.create(Type.STRING);
//         case INT16:
//         case INT32:
//             return org.apache.avro.Schema.create(Type.INT);
//         case INT64:
//             return org.apache.avro.Schema.create(Type.LONG);
//         case FLOAT:
//             return org.apache.avro.Schema.create(Type.FLOAT);
//         case DOUBLE:
//             return org.apache.avro.Schema.create(Type.DOUBLE);
//         case BOOLEAN:
//             return org.apache.avro.Schema.create(Type.BOOLEAN);
//         // Add more cases as needed for other Beam types
//         default:
//             throw new IllegalArgumentException("Unsupported Beam type: " + beamType.getTypeName());
//     }
// }
  public interface BigQueryToTextOptions extends PipelineOptions {
    /** Set this required option to specify where to write the text output. */
    @Description("Path of the file to write to")
    @Required
    String getOutput();
    void setOutput(String value);
  }

  public static void main(String[] args) {

    BigQueryToTextOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryToTextOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<TableRow> data = pipeline
      .apply(
        "Read from BigQuery query",
        BigQueryIO.readTableRowsWithSchema().fromQuery("select create_yearmonth, biz_workplace_name, biz_regid, register_status from `looker_dataset.national_pension_mom` limit 10").usingStandardSql()
      );
    
    data.apply("output to stream",ParDo.of(new DoFn<TableRow, String>() {
      @ProcessElement
      public void ProcessElement(@Element TableRow inRow, OutputReceiver<String> out) {
        try {
          out.output(inRow.toPrettyString());
        } catch(Exception e)
        {
          e.printStackTrace();
        }
      }
    }))
    .apply("output to system console", TextIO.write().to(options.getOutput()));
    /*
GenericData{classInfo=[f], {create_yearmonth=2022-06, biz_workplace_name=���뚮��숉삊�숈“��, biz_regid=210817, register_status=1}}
GenericData{classInfo=[f], {create_yearmonth=2022-06, biz_workplace_name=二쇱떇�뚯궗�뺤긽遺곹븳�곕━議고듃, biz_regid=506853, register_status=1}}
GenericData{classInfo=[f], {create_yearmonth=2022-02, biz_workplace_name=�곗씠�좎꽕寃쎌쟾泥좎슫�곸＜�앺쉶��, biz_regid=834870, register_status=1}}
     */
    pipeline.run();
  }
}
