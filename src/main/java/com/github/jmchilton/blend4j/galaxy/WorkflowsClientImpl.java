package com.github.jmchilton.blend4j.galaxy;

import java.util.List;

import org.codehaus.jackson.type.TypeReference;

import com.github.jmchilton.blend4j.galaxy.beans.Workflow;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowDetails;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInputs;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInvocation;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInvocationInputs;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowOutputs;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

class WorkflowsClientImpl extends Client implements WorkflowsClient {
  public WorkflowsClientImpl(GalaxyInstanceImpl galaxyInstance) {
    super(galaxyInstance, "workflows");
  }

  public List<Workflow> getWorkflows() {
    return get(new TypeReference<List<Workflow>>() {
    });
  }

  public ClientResponse showWorkflowResponse(final String id) {
    return super.show(id, ClientResponse.class);
  }

  public WorkflowDetails showWorkflow(final String id) {
    return super.show(id, WorkflowDetails.class);
  }

  public String exportWorkflow(final String id) {
    WebResource webResource = getWebResource().path("download").path(id);
    return webResource.get(String.class);
  }

  @Deprecated
  public ClientResponse runWorkflowResponse(WorkflowInputs workflowInputs) {
    return super.create(workflowInputs);
  }

  @Deprecated
  public WorkflowOutputs runWorkflow(final WorkflowInputs workflowInputs) {
    return runWorkflowResponse(workflowInputs).getEntity(WorkflowOutputs.class);
  }
  
  public ClientResponse invokeWorkflowResponse(WorkflowInvocationInputs workflowInputs) {
	WebResource webResource = getWebResource().path(workflowInputs.getWorkflowId()).path("invocations");
    return super.create(webResource, workflowInputs);
  }

  public WorkflowInvocation invokeWorkflow(final WorkflowInvocationInputs workflowInputs) {
    return invokeWorkflowResponse(workflowInputs).getEntity(WorkflowInvocation.class);
  }
  
  public WorkflowInvocation showInvocation(String workflowId, String invocationId) {
	  ///GET to api/workflows/{workflow_id}/invocations/{invocation_id}
	  WebResource webResource = getWebResource().path(workflowId).path("invocations").path(invocationId);
	  return super.getResponse(webResource).getEntity(WorkflowInvocation.class);
  }

  public ClientResponse importWorkflowResponse(final String json) {
    final String payload = String.format("{\"workflow\": %s}", json);
    return create(getWebResource().path("upload"), payload);
  }

  public Workflow importWorkflow(String json) {
    return importWorkflowResponse(json).getEntity(Workflow.class);
  }

  @Override
  public ClientResponse deleteWorkflowRequest(String id) {
    return deleteResponse(getWebResource(id));
  }
}
