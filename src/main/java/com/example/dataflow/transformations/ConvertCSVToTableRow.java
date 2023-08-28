package com.example.dataflow.transformations;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.dataflow.constants.ERRORTYPE;
import com.example.dataflow.model.BigQueryProcessError;
import com.google.api.services.bigquery.model.TableRow;

public class ConvertCSVToTableRow extends DoFn<String, TableRow> {

	private TupleTag<TableRow> success;
	private TupleTag<BigQueryProcessError> DEADLETTER_OUT;
	private static final long serialVersionUID = 1L;
	private String headers;
	private static final String commaSeperator = ",";

	private static final Logger logger = LoggerFactory.getLogger(ConvertCSVToTableRow.class);

	public ConvertCSVToTableRow(TupleTag<TableRow> success,
			TupleTag<BigQueryProcessError> DEADLETTER_OUT, String headers) {
		this.success = success;
		this.DEADLETTER_OUT = DEADLETTER_OUT;
		this.headers = headers;
	}

	@ProcessElement
	public void processElement(ProcessContext context) {
		String record = null;
		TableRow tableRow = null;
		try {
			record  = context.element();
			
			if(!record.contains(headers)) {
				tableRow = convertJsonToTableRow(record, context);
			}
			context.output(success, tableRow);
		} catch (Exception e) {
			logger.error("Failed transform: " + e.getMessage(), e);
			context.output(DEADLETTER_OUT, new BigQueryProcessError(record, e.getCause().toString(),
					ERRORTYPE.TransformationError.toString(), ""));

		}
}
	public  TableRow convertJsonToTableRow(String record, ProcessContext context) {

		TableRow row = new TableRow();
		String[] parts = record.split(commaSeperator);
		String[] columnNames = headers.split(commaSeperator);

		try {
				for (int i = 0; i < parts.length; i++) {
					/*
					 * if(columnNames[i]!="Name" || columnNames[i]!="LastName") {
					 * row.set(columnNames[i], parts[i]); } else { fullName = parts[1] + parts[2];
					 * row.set("FullName", fullName); }
					 */
					row.set(columnNames[i], parts[i]);
				}
		} catch (Exception e) {
			throw new RuntimeException("Failed to serialize json to table row: " + record, e);
		}
		return row;
	}
}
