package com.example.dataflow.transformations;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.dataflow.constants.AuditColumns;
import com.example.dataflow.constants.ERRORLOGSTREAM;
import com.example.dataflow.model.BigQueryProcessError;
import com.google.api.services.bigquery.model.TableRow;

public class BigQueryProcessErrors extends DoFn<BigQueryProcessError, TableRow>{
	
	private static final Logger logger = LoggerFactory.getLogger(BigQueryProcessErrors.class);
	private ValueProvider<String> TableName;
	private String ccnName;
	private String sourceSystemName;
	private String eventTableName;

	public BigQueryProcessErrors(ValueProvider<String> TableName, String ccnName, String sourceSystemName,
			String eventTableName) {
		this.TableName = TableName;
		this.ccnName = ccnName;
		this.sourceSystemName = sourceSystemName;
		this.eventTableName =eventTableName;
	}
	
	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext context) 
	{
		BigQueryProcessError ERROR = context.element();
		String actualMsg = ERROR.getOriginalMsg();
		String topicName = ERROR.getTopicName();
		String insertErrors = ERROR.getErrorMessage();
		JSONObject jsonMessage = null;
		String jsonValidity = null;
		
		
		TableRow row = new TableRow();
		
		// removing the extra forward slashes
		
		try {
			jsonMessage = new JSONObject(ERROR.getOriginalMsg().replace("\\",""));
			jsonValidity = "Valid JSON";
		}catch(Exception e) {
			jsonValidity = "Invalid JSON";
			logger.error("BigQueryProcessErrors : Invalid JSON Messages");
		}
		
		logger.error("BigQueryProcessErrors: Error occured for Topic :-{}\n"
				+ "Actual Message:-{}\n"
				+ "Issue with the JSON schema of original Message : Reason -{} ",topicName,actualMsg,insertErrors);
		
		if (sourceSystemName.equalsIgnoreCase("")) {
			if(ERROR.getErrorMessage().contains("ExtraFieldException")) {
				row.set("Id", jsonMessage.get("Id"));
			}else {
				row.set("Id", "Not a valid message");
			}	
		}
		
		row.set(ERRORLOGSTREAM.message_body.toString(), ERROR.getOriginalMsg());
		row.set(ERRORLOGSTREAM.error_detail.toString(), ERROR.getErrorMessage());
		row.set(ERRORLOGSTREAM.src_sys_nm.toString(), sourceSystemName);
		
		row.set(ERRORLOGSTREAM.src_component.toString(), AuditColumns.KAFKA.toString());
		row.set(ERRORLOGSTREAM.src_object_nm.toString(), ERROR.getTopicName());
		row.set(ERRORLOGSTREAM.target_component.toString(), AuditColumns.BQ.toString());
		row.set(ERRORLOGSTREAM.target_object_nm.toString(), TableName.get());
		row.set(ERRORLOGSTREAM.gcp_ingst_dt.toString(), LocalDateTime.now(ZoneOffset.UTC).toString());

		context.output(row);
	}
}
