package com.example.dataflow.tableoptions;

import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface OptionsForCSV extends PipelineOptions, StreamingOptions,DataflowPipelineWorkerPoolOptions {

	
	@Description("Project ID")
	String getProjectID();
	void setProjectID(String projectID);

	@Description("Output table specs")
	@Default.String("")
	ValueProvider<String> getOutputTable();
	void setOutputTable(ValueProvider<String> outputTable);
	
	@Description("error table specifications")
	@Default.String("")
	ValueProvider<String> getErrorSpec();
	void setErrorSpec(ValueProvider<String> errorSpec);
	
	
	@Description("Source CSV file path ")
	String getSourceFilePath();
	void setSourceFilePath(String sourceFilePath);
	
	
	@Description("csv header")
	String getCsvHeaders();
	void setCsvHeaders(String csvHeaders);
	
	@Description("CSV source name")
	String getSourceName();
	void setSourceName(String sourceName);

}
