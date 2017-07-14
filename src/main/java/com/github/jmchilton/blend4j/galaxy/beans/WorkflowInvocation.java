package com.github.jmchilton.blend4j.galaxy.beans;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowInvocation {

	private String historyId, id, state, workflowId, updateTime;
	
	private List<Step> steps;
	
	private UUID uuid;

	public String getHistoryId() {
		return historyId;
	}

	@JsonProperty("history_id")
	public void setHistoryId(final String historyId) {
		this.historyId = historyId;
	}

	public String getId() {
		return id;
	}

	@JsonProperty("id")
	public void setId(String id) {
		this.id = id;
	}

	public String getState() {
		return state;
	}

	@JsonProperty("state")
	public void setState(String state) {
		this.state = state;
	}

	public String getWorkflowId() {
		return workflowId;
	}

	@JsonProperty("workflow_id")
	public void setWorkflowId(String workflowId) {
		this.workflowId = workflowId;
	}

	public UUID getUUID() {
		return uuid;
	}

	@JsonProperty("uuid")
	public void setUUID(String uuid) {
		this.uuid = UUID.fromString(uuid);
	}
	
	
	public String getUpdateTime() {
		return updateTime;
	}
	
	@JsonProperty("update_time")
	public void setUpdateTime(String updateTime) {
		//would like to use a Date object internally but couldn't get the parsing right
		this.updateTime = updateTime;	
	}
	
	public List<Step> getWorkflowSteps() {
		return steps;
	}
	
	@JsonProperty("steps")
	public void setWorkflowSteps(List<Step> steps) {		
		this.steps = steps;
		Collections.sort(steps);
	}
	
	
	
	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class Step implements Comparable<Step> {
		String state, updateTime, jobId, id, label;
		
		int orderIndex;
		
		UUID uuid;

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
		
		@JsonProperty("workflow_step_id")
		public void setId(String id) {
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
		public int compareTo(Step other) {
			return orderIndex - other.orderIndex;
		}
	}
}
