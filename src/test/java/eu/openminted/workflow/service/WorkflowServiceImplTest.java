package eu.openminted.workflow.service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.junit.Test;

import eu.openminted.registry.domain.Component;
import eu.openminted.registry.domain.ComponentInfo;
import eu.openminted.registry.domain.IdentificationInfo;
import eu.openminted.registry.domain.MetadataHeaderInfo;
import eu.openminted.registry.domain.MetadataIdentifier;
import eu.openminted.registry.domain.ResourceIdentifier;
import eu.openminted.store.restclient.StoreRESTClient;
import eu.openminted.workflow.api.ExecutionStatus;
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

		String executionID = startWorkflow(workflowService, "ANNIE", "/input/SingleDocument/");

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

	@Test
	public void testMultipleDocumentANNIEWorkflow() throws WorkflowException, InterruptedException {
		WorkflowServiceImpl workflowService = new WorkflowServiceImpl();
		workflowService.galaxyInstanceUrl = "http://localhost:8899";
		workflowService.galaxyApiKey = "4454f8849b3d30e1a6551727f871dbd7";

		String executionID = startWorkflow(workflowService, "ANNIE", "/input/MultipleDocuments/");

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
	
	@Test
	public void testMissingWorkflow() throws WorkflowException, InterruptedException {
		WorkflowServiceImpl workflowService = new WorkflowServiceImpl();
		workflowService.galaxyInstanceUrl = "http://localhost:8899";
		workflowService.galaxyApiKey = "4454f8849b3d30e1a6551727f871dbd7";

		String executionID = startWorkflow(workflowService, "MissingWorkflow", "/input/SingleDocument/");

		ExecutionStatus status = null;

		while (true) {
			status = workflowService.getExecutionStatus(executionID);
			if (status.getStatus().equals(Status.FINISHED) || status.getStatus().equals(Status.FAILED)
					|| status.getStatus().equals(Status.CANCELED)) {
				break;
			}

			Thread.sleep(200L);
		}

		assertEquals(Status.FAILED, status.getStatus());
		assertNull(status.getCorpusID());
		assertNotNull(status.getFailureCause());
	}

	@Test
	public void testCancelWorkflow() throws WorkflowException, InterruptedException {
		WorkflowServiceImpl workflowService = new WorkflowServiceImpl();
		workflowService.galaxyInstanceUrl = "http://localhost:8899";
		workflowService.galaxyApiKey = "4454f8849b3d30e1a6551727f871dbd7";

		String executionID = startWorkflow(workflowService, "ANNIE", "/input/MultipleDocuments/");
		workflowService.cancel(executionID);

		ExecutionStatus status = null;

		while (true) {
			status = workflowService.getExecutionStatus(executionID);
			if (status.getStatus().equals(Status.FINISHED) || status.getStatus().equals(Status.FAILED)
					|| status.getStatus().equals(Status.CANCELED)) {
				break;
			}

			Thread.sleep(200L);
		}

		assertEquals(Status.CANCELED, status.getStatus());
		assertNull(status.getCorpusID());
		assertNull(status.getFailureCause());
	}

	@Test
	public void testUseStore() throws URISyntaxException, IOException, WorkflowException, InterruptedException {
		WorkflowServiceImpl workflowService = new WorkflowServiceImpl();
		workflowService.galaxyInstanceUrl = "http://localhost:8899";
		workflowService.galaxyApiKey = "4454f8849b3d30e1a6551727f871dbd7";
		workflowService.storeEndpoint = "http://localhost:8898";

		StoreRESTClient storeClient = new StoreRESTClient(workflowService.storeEndpoint);

		Path archiveData = Paths.get(WorkflowServiceImplTest.class.getResource("/input/MultipleDocuments/").toURI());

		String corpusId = uploadArchive(storeClient, archiveData);

		String executionID = startWorkflow(workflowService, "ANNIE", corpusId);

		ExecutionStatus status = null;

		while (true) {
			status = workflowService.getExecutionStatus(executionID);
			if (status.getStatus().equals(Status.FINISHED) || status.getStatus().equals(Status.FAILED)) {
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

	private String uploadArchive(StoreRESTClient storeClient, Path archiveData) throws IOException {
		String archiveID = storeClient.createArchive().getResponse();
		String annotationsFolderId = storeClient.createSubArchive(archiveID, "fulltext").getResponse();

		Files.walk(archiveData).filter(path -> !Files.isDirectory(path)).forEach(path -> {
			storeClient.storeFile(path.toFile(), annotationsFolderId, path.getFileName().toString());
		});

		storeClient.finalizeArchive(archiveID);

		return archiveID;
	}
}
