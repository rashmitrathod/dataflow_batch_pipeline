package com.example.dataflow.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.dataflow.model.BigQueryProcessError;
import com.example.dataflow.tableoptions.OptionsForCSV;
import com.example.dataflow.transformations.TableRowBasedOnSource;
import com.google.api.services.bigquery.model.TableRow;

public class GcsToBigQuery {

	private static final Logger logger = LoggerFactory.getLogger(GcsToBigQuery.class);
	private static String fullName = null;
	private static final String headerOptionsDelimiter = "~";
	private static final String commaSeperator = ",";

	
	static final TupleTag<BigQueryProcessError> DEADLETTER_OUT = new TupleTag<BigQueryProcessError>() {
		private static final long serialVersionUID = 1L;
	};

	static final TupleTag<TableRow> success = new TupleTag<TableRow>() {
		private static final long serialVersionUID = 1L;
	};
	 
	public static void main(String[] args) {

		OptionsForCSV optionsForCSV = PipelineOptionsFactory.fromArgs(args).withValidation().as(OptionsForCSV.class);
		System.out.println("Options initialize...");
		
		String HEADERS = optionsForCSV.getCsvHeaders().replace(headerOptionsDelimiter, commaSeperator);

		Pipeline pipeline = Pipeline.create(optionsForCSV);

		PCollection<String> csvMessages = pipeline
				.apply("Read CSV File", TextIO.read().from(optionsForCSV.getSourceFilePath()))
				.apply("Log messages", ParDo.of(new DoFn<String, String>() {
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) {
						System.out.println("Element is :" + c.element());
						c.output(c.element());
					}
				}));
		
		// Converting to tableRow based on source system (BigQuery Understandable format)
		PCollectionTuple tablesRow = csvMessages.apply("Read lines",
				(new TableRowBasedOnSource(success, DEADLETTER_OUT, HEADERS, optionsForCSV.getSourceName())));

		// write result to success table
		WriteResult result = tablesRow.get(success).apply("Write to success BigQuery",
				BigQueryIO.writeTableRows().withExtendedErrorInfo().ignoreUnknownValues()
						.to(optionsForCSV.getOutputTable()).withCreateDisposition(CreateDisposition.CREATE_NEVER)
						.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
						.withWriteDisposition(WriteDisposition.WRITE_APPEND));

		// insert errors into BQ table
		/*result.getFailedInsertsWithErr()
				.apply("convert data errors to tables rows",
						ParDo.of(new FailedInsertRecords(optionsForCSV.getOutputTable(), "",
								optionsForCSV.getSourceName(), "")))
				.apply("tables rows to BQ error table",
						BigQueryIO.writeTableRows().to(optionsForCSV.getErrorSpec())
								.withCreateDisposition(CreateDisposition.CREATE_NEVER)
								.withWriteDisposition(WriteDisposition.WRITE_APPEND));*/
				
		pipeline.run();
	}
	
	
	 /** Defines the BigQuery schema used for the output. */
   /* static TableSchema getSchema() {
         List<TableFieldSchema> fields = new ArrayList<>();
         // Currently store all values as String
         fields.add(new TableFieldSchema().setName("ID").setType("STRING"));
         fields.add(new TableFieldSchema().setName("FullName").setType("STRING"));
         fields.add(new TableFieldSchema().setName("Marks").setType("STRING"));
         fields.add(new TableFieldSchema().setName("Percentage").setType("STRING"));
        return new TableSchema().setFields(fields);
     } */
	
}
