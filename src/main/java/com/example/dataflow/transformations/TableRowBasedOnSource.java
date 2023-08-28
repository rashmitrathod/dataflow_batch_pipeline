package com.example.dataflow.transformations;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.example.dataflow.model.BigQueryProcessError;
import com.google.api.services.bigquery.model.TableRow;

public class TableRowBasedOnSource extends PTransform<PCollection<String>, PCollectionTuple> {

	private TupleTag<TableRow> success;
	private TupleTag<BigQueryProcessError> DEADLETTER_OUT;
	private String sourceName;
	private static final String STUDENTDATA = "studentData";
	private String headers;

	public TableRowBasedOnSource(TupleTag<TableRow> success, TupleTag<BigQueryProcessError> DEADLETTER_OUT,
			 String headers, String sourceName) { 
		this.success = success;
		this.DEADLETTER_OUT = DEADLETTER_OUT;
		this.headers = headers;
		this.sourceName = sourceName;
	}

	private static final long serialVersionUID = 1L;

	@Override
	public PCollectionTuple expand(PCollection<String> csvMessages) {
		PCollectionTuple tablesRow = null;
		if (sourceName.equalsIgnoreCase(STUDENTDATA)) {
			 tablesRow = csvMessages
						.apply("Check Source Name",
								ParDo.of(new ConvertCSVToTableRow(success, DEADLETTER_OUT, headers))
										.withOutputTags(success, TupleTagList.of(DEADLETTER_OUT)));
		} 
		return tablesRow;
	}

}
