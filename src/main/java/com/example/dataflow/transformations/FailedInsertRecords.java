package com.example.dataflow.transformations;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.dataflow.constants.AuditColumns;
import com.example.dataflow.constants.ERRORLOGSTREAM;
import com.example.dataflow.transformations.FailedInsertRecords;
import com.google.api.services.bigquery.model.ErrorProto;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;

public class FailedInsertRecords extends DoFn<BigQueryInsertError, TableRow> {

	private static final Logger logger = LoggerFactory.getLogger(FailedInsertRecords.class);
	private ValueProvider<String> TableName;
	private String ccnName;
	private String sourceSystemName;
	String id = null;
	private String eventTableName;

	public FailedInsertRecords(ValueProvider<String> TableName, String ccnName, String sourceSystemName,
			String eventTableName) {
		this.TableName = TableName;
		this.ccnName = ccnName;
		this.sourceSystemName = sourceSystemName;
		this.eventTableName=eventTableName;
	}

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext context) {
		BigQueryInsertError ERROR = context.element();
		TableRow actualMsg = ERROR.getRow();
		String topicName = (String) actualMsg.get(AuditColumns.SRC_TOPIC_NM.toString());
		
		/*Logic to populate the Id column in Deadletter table*/
		if (sourceSystemName.equalsIgnoreCase("")) {
			id = (String) actualMsg.get("id");		
		}
		
		Gson gson = new Gson();
		String actualjson = gson.toJson(actualMsg);
		InsertErrors insertErrors = ERROR.getError();
		List<ErrorProto> errorProtoList = insertErrors.getErrors();
		ErrorProto errorProto = errorProtoList.get(0);
		TableRow row = new TableRow();

		logger.error(
				"FailedInsertRecords: Error occured for Topic :-{}\n" + "Actual Message:-{}\n"
						+ "Error While inserting into BigQuery : Reason -{} ",
				topicName, actualjson, errorProto.getMessage());
		row.set("Id", id);
		row.set(ERRORLOGSTREAM.src_sys_nm.toString(), sourceSystemName);
		row.set(ERRORLOGSTREAM.src_component.toString(), AuditColumns.KAFKA);
		row.set(ERRORLOGSTREAM.src_object_nm.toString(), topicName);

		row.set(ERRORLOGSTREAM.target_component.toString(), AuditColumns.BQ);
		row.set(ERRORLOGSTREAM.target_object_nm.toString(), TableName.get());
		row.set(ERRORLOGSTREAM.message_body.toString(), actualjson);
		row.set(ERRORLOGSTREAM.error_detail.toString(), errorProto.getMessage());
		row.set(ERRORLOGSTREAM.gcp_ingst_dt.toString(), LocalDateTime.now(ZoneOffset.UTC).toString());

		// row.set(ERRORLOGSTREAM.error_code.toString(), "error codes");
		// row.set(ERRORLOGSTREAM.action_taken.toString(), "Null or re-process");

		context.output(row);
	}

}
