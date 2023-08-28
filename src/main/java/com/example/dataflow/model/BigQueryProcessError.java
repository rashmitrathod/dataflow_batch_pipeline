package com.example.dataflow.model;

import java.io.Serializable;

public class BigQueryProcessError implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String originalMsg;
	private String errorMessage;
	private String ErrorType;
	private String topicName;
	
	public BigQueryProcessError(String originalMsg, String errorMessage, String errorType, String topicName) {
		super();
		this.originalMsg = originalMsg;
		this.errorMessage = errorMessage;
		ErrorType = errorType;
		this.topicName = topicName;
	}
	
	public String getTopicName() {
		return topicName;
	}

	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public String getOriginalMsg() {
		return originalMsg;
	}

	public void setOriginalMsg(String originalMsg) {
		this.originalMsg = originalMsg;
	}

	public String getErrorMessage() {
		return errorMessage;
	}
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}
	public String getErrorType() {
		return ErrorType;
	}
	public void setErrorType(String errorType) {
		ErrorType = errorType;
	}
}
