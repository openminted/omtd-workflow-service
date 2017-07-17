package com.github.jmchilton.blend4j.galaxy.beans;

import java.util.UUID;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowInvocationStepOutput {
	WorkflowInputs.InputSourceType type;
	
	String id;
	
	UUID uuid;
	
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
	
	public WorkflowInputs.InputSourceType getType() {
		return type;
	}
	
	@JsonProperty("src")
	public void setType(String type) {
		//the toUpperCase gives me the heeby geebys
		this.type = WorkflowInputs.InputSourceType.valueOf(type.toUpperCase());
	}
}
