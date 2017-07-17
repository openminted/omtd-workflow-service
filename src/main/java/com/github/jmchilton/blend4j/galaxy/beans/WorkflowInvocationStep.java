package com.github.jmchilton.blend4j.galaxy.beans;

import java.util.Map;
import java.util.UUID;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowInvocationStep implements Comparable<WorkflowInvocationStep> {
	String state, updateTime, jobId, id, label, workflowStepId;
	
	int orderIndex;
	
	UUID uuid;
	
	Map<String,WorkflowInvocationStepOutput> outputs;

	public Map<String, WorkflowInvocationStepOutput> getOutputs() {
		return outputs;
	}

	public void setOutputs(Map<String, WorkflowInvocationStepOutput> outputs) {
		this.outputs = outputs;
	}

	public String getState() {
		return state;
	}

	@JsonProperty("state")
	public void setState(String state) {
		this.state = state;
	}
	
	public UUID getUUID() {
		return uuid;
	}
	
	@JsonProperty("workflow_step_uuid")
	public void setUUID(String uuid) {
		this.uuid = UUID.fromString(uuid);
	}
	
	public String getId() {
		return id;
	}
	
	@JsonProperty("id")
	public void setId(String id) {
		this.id = id;
	}
	
	public String getWorkflowStepId() {
		return id;
	}
	
	@JsonProperty("workflow_step_id")
	public void setWorkflowStepId(String id) {
		this.id = id;
	}
			
	public String getLabel() {
		return label;
	}

	@JsonProperty("workflow_step_label")
	public void setLabel(String label) {
		this.label = label;
	}

	public String getUpdateTime() {
		return updateTime;
	}
	
	@JsonProperty("update_time")
	public void setUpdateTime(String updateTime) {
		//would like to use a Date object internally but couldn't get the parsing right
		this.updateTime = updateTime;	
	}
	
	public String getJobId() {
		return jobId;
	}
	
	@JsonProperty("job_id")
	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	
	public int getOrderIndex() {
		return orderIndex;
	}
	
	@JsonProperty("order_index")
	public void setOrderIndex(int orderIndex) {
		this.orderIndex = orderIndex;
	}

	@Override
	public int compareTo(WorkflowInvocationStep other) {
		return orderIndex - other.orderIndex;
	}
}