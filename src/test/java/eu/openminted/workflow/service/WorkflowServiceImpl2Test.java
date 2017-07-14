package eu.openminted.workflow.service;

import org.junit.Test;

import eu.openminted.registry.domain.Component;
import eu.openminted.registry.domain.MetadataHeaderInfo;
import eu.openminted.registry.domain.MetadataIdentifier;
import eu.openminted.workflow.api.ExecutionStatus;
import eu.openminted.workflow.api.ExecutionStatus.Status;
import eu.openminted.workflow.api.WorkflowException;
import eu.openminted.workflow.api.WorkflowJob;
import eu.openminted.workflow.api.WorkflowService;
import junit.framework.TestCase;

public class WorkflowServiceImpl2Test extends TestCase {

	@Test
	public void testMultipleDocumentPDFWorkflow() throws WorkflowException, InterruptedException {
		WorkflowServiceImpl2 workflowService = new WorkflowServiceImpl2();
		workflowService.galaxyInstanceUrl = "http://snf-754063.vm.okeanos.grnet.gr/";
		workflowService.galaxyApiKey = "36ea7fa29c38b9144dded51957b22ddb";

		String executionID = startWorkflow(workflowService, "MGTest1", "/input/PDFs/");

		ExecutionStatus status = null;

		while (true) {
			status = workflowService.getExecutionStatus(executionID);
			if (status.getStatus().equals(Status.FINISHED) || status.getStatus().equals(Status.FAILED)
					|| status.getStatus().equals(Status.CANCELED)) {
				break;
			}

			Thread.sleep(200L);
		}

		assertEquals(Status.FINISHED, status.getStatus());
		assertNotNull(status.getCorpusID());
		assertNull(status.getFailureCause());
	}

	private String startWorkflow(WorkflowService workflowService, String workflowID, String corpusID)
			throws WorkflowException {

		MetadataIdentifier metadataId = new MetadataIdentifier();
		metadataId.setValue(workflowID);
		
		MetadataHeaderInfo metadataHeaderInfo = new MetadataHeaderInfo();		
		metadataHeaderInfo.setMetadataRecordIdentifier(metadataId);
		
		Component workflow = new Component();
		workflow.setMetadataHeaderInfo(metadataHeaderInfo);
		
		WorkflowJob workflowJob = new WorkflowJob();
		workflowJob.setWorkflow(workflow);

		if (corpusID.startsWith("/")) {
			workflowJob.setCorpusId(this.getClass().getResource(corpusID).toString());
		} else {
			workflowJob.setCorpusId(corpusID);
		}

		// System.out.println("about to execute");
		return workflowService.execute(workflowJob);

	}
}
