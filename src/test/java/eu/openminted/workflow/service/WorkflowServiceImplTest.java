package eu.openminted.workflow.service;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;

import org.junit.Test;

import eu.openminted.registry.domain.Component;
import eu.openminted.registry.domain.ComponentInfo;
import eu.openminted.registry.domain.IdentificationInfo;
import eu.openminted.registry.domain.ResourceIdentifier;
import eu.openminted.workflow.api.ExecutionStatus.Status;
import eu.openminted.workflow.api.WorkflowException;
import eu.openminted.workflow.api.WorkflowJob;
import eu.openminted.workflow.api.WorkflowService;
import junit.framework.TestCase;

public class WorkflowServiceImplTest extends TestCase {

	@Test
	public void testSingleDocumentANNIEWorkflow() throws WorkflowException, InterruptedException {
		WorkflowServiceImpl workflowService = new WorkflowServiceImpl();
		workflowService.galaxyInstanceUrl = "http://localhost:8899";
		workflowService.galaxyApiKey = "4454f8849b3d30e1a6551727f871dbd7";

		String executionID = startWorkflow(workflowService,"ANNIE","/input/SingleDocument/");
		
		Status status = null;
		
		while (true) {
			status = workflowService.getExecutionStatus(executionID).getStatus();
			if (status.equals(Status.FINISHED) || status.equals(Status.FAILED)) {
				break;
			}

			Thread.sleep(200L);
		}
		
		assertEquals(Status.FINISHED, status);
	}

	@Test
	public void testMultipleDocumentANNIEWorkflow() throws WorkflowException, InterruptedException {
		WorkflowServiceImpl workflowService = new WorkflowServiceImpl();
		workflowService.galaxyInstanceUrl = "http://localhost:8899";
		workflowService.galaxyApiKey = "4454f8849b3d30e1a6551727f871dbd7";
		
		String executionID = startWorkflow(workflowService, "ANNIE", "/input/MultipleDocuments/");

		Status status = null;
		
		while (true) {
			status = workflowService.getExecutionStatus(executionID).getStatus();
			if (status.equals(Status.FINISHED) || status.equals(Status.FAILED)) {
				break;
			}

			Thread.sleep(200L);
		}
		
		assertEquals(Status.FINISHED, status);
	}
	
	@Test
	public void testCancelWorkflow() throws WorkflowException, InterruptedException {
		WorkflowServiceImpl workflowService = new WorkflowServiceImpl();
		workflowService.galaxyInstanceUrl = "http://localhost:8899";
		workflowService.galaxyApiKey = "4454f8849b3d30e1a6551727f871dbd7";
		
		String executionID = startWorkflow(workflowService, "ANNIE", "/input/MultipleDocuments/");
		workflowService.cancel(executionID);

		Status status = null;
		
		while (true) {
			status = workflowService.getExecutionStatus(executionID).getStatus();
			if (!status.equals(Status.PENDING) && !status.equals(Status.RUNNING)) {
				break;
			}

			Thread.sleep(200L);
		}
		
		assertEquals(Status.CANCELED, status);
	}
	
	private String startWorkflow(WorkflowService workflowService, String workflowID, String corpusID) throws WorkflowException {
		
		// This can't possibly be the right way to set the ID of the workflow we
		// want to run. Who codes something five levels deep just to get a
		// single ID, especially as you end up at a list!
		ResourceIdentifier workflowId = new ResourceIdentifier();
		workflowId.setValue(workflowID);

		IdentificationInfo workflowIdInfo = new IdentificationInfo();
		workflowIdInfo.setIdentifiers(Arrays.asList(new ResourceIdentifier[] { workflowId }));

		ComponentInfo workflowInfo = new ComponentInfo();
		workflowInfo.setIdentificationInfo(workflowIdInfo);

		Component workflow = new Component();
		workflow.setComponentInfo(workflowInfo);

		WorkflowJob workflowJob = new WorkflowJob();
		workflowJob.setWorkflow(workflow);

		workflowJob.setCorpusId(this.getClass().getResource(corpusID).toString());

		//System.out.println("about to execute");
		return workflowService.execute(workflowJob);

	}
}
