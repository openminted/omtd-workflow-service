package eu.openminted.workflow.service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.github.jmchilton.blend4j.galaxy.GalaxyInstance;
import com.github.jmchilton.blend4j.galaxy.GalaxyInstanceFactory;
import com.github.jmchilton.blend4j.galaxy.HistoriesClient;
import com.github.jmchilton.blend4j.galaxy.ToolsClient;
import com.github.jmchilton.blend4j.galaxy.WorkflowsClient;
import com.github.jmchilton.blend4j.galaxy.beans.History;
import com.github.jmchilton.blend4j.galaxy.beans.HistoryDeleteResponse;
import com.github.jmchilton.blend4j.galaxy.beans.ToolExecution;
import com.github.jmchilton.blend4j.galaxy.beans.ToolInputs;
import com.github.jmchilton.blend4j.galaxy.beans.Workflow;

import eu.openminted.registry.domain.Component;
import eu.openminted.registry.domain.MetadataHeaderInfo;
import eu.openminted.registry.domain.MetadataIdentifier;
import eu.openminted.store.restclient.StoreRESTClient;
import eu.openminted.workflow.api.ExecutionStatus;
import eu.openminted.workflow.api.ExecutionStatus.Status;
import eu.openminted.workflow.api.WorkflowException;
import eu.openminted.workflow.api.WorkflowJob;
import eu.openminted.workflow.api.WorkflowService;
import junit.framework.TestCase;

public class WorkflowServiceImplTest extends TestCase {

	@Test
	public void testMultipleDocumentPDFWorkflow() throws WorkflowException, InterruptedException {
		WorkflowServiceImpl workflowService = new WorkflowServiceImpl();
		workflowService.galaxyInstanceUrl = "http://snf-754063.vm.okeanos.grnet.gr/";
		workflowService.galaxyApiKey = "36ea7fa29c38b9144dded51957b22ddb";
		// workflowService.galaxyInstanceUrl = "http://localhost:8080/";
		// workflowService.galaxyApiKey = "0403419ce354f40ff6503176c81ebbaf";

		String executionID = startWorkflow(workflowService, "MAGLatest1", "/input/PDFs/");

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

	@Test
	public void testDeleteWorkflow() throws Exception {
		MetadataIdentifier metadataId = new MetadataIdentifier();
		metadataId.setValue("ToBeDeleted");

		MetadataHeaderInfo metadataHeaderInfo = new MetadataHeaderInfo();
		metadataHeaderInfo.setMetadataRecordIdentifier(metadataId);

		Component workflow = new Component();
		workflow.setMetadataHeaderInfo(metadataHeaderInfo);

		WorkflowServiceImpl service = new WorkflowServiceImpl();
		service.galaxyApiKey = "0403419ce354f40ff6503176c81ebbaf";
		service.galaxyInstanceUrl = "http://localhost:8080";

		service.delete(workflow);

	}
	
	@Test
	public void testHistoryDeletion() throws Exception {
		GalaxyInstance galaxyInstance = GalaxyInstanceFactory.get("http://localhost:8080",
				"0403419ce354f40ff6503176c81ebbaf");
		
		HistoriesClient historiesClient = galaxyInstance.getHistoriesClient();
		
		History history = new History("Deletion Test " + (new Date()));
		history = historiesClient.create(history);
		
		HistoryDeleteResponse deleteResponse = historiesClient.deleteHistory(history.getId());
		
		assertTrue("hisotry not deleted", deleteResponse.getDeleted());
		assertFalse("hisotry purged when it should only have been deleted", deleteResponse.getPurged());
		
		deleteResponse = historiesClient.deleteHistory(history.getId(),true);
		
		assertTrue("hisotry not deleted", deleteResponse.getDeleted());
		assertTrue("hisotry not purged", deleteResponse.getPurged());
	}
	
	@Test
	public void testWorkflowCreation() throws Exception {
		GalaxyInstance galaxy = GalaxyInstanceFactory.get("http://localhost:8080",
				"0403419ce354f40ff6503176c81ebbaf");
		
		WorkflowsClient workflowsClient = galaxy.getWorkflowsClient();
		
		Workflow workflow = workflowsClient.createWorkflow("OMTD Workflow "+(new Date()));
		System.out.println(workflow.getId());
	}

	@Test
	public void testFTPUploads() throws Exception {
		GalaxyInstance galaxyInstance = GalaxyInstanceFactory.get("http://localhost:8080",
				"0403419ce354f40ff6503176c81ebbaf");

		// You need to enable FTP in galaxy by specifying a direcotry for
		// ftp_upload_dir in config/galaxy.ini

		HistoriesClient historiesClient = galaxyInstance.getHistoriesClient();
		ToolsClient toolsClient = galaxyInstance.getToolsClient();

		History history = new History("FTP Test " + (new Date()));
		history = historiesClient.create(history);

		System.out.println("History ID: " + history.getId());

		// we need a map to hold the options which will become a JSON map
		Map<String, String> ftpUploadSettings = new HashMap<String, String>();

		// specify that we are uploading a dataset
		ftpUploadSettings.put("files_0|type", "upload_dataset");

		// relative path within the FTP folder for the current user to the file
		// we want to add to the history
		ftpUploadSettings.put("files_0|ftp_files", "document001.txt");

		// this must reference upload1 as that seems to be the internal name for
		// the tool in Galaxy
		ToolInputs ftpUpload = new ToolInputs("upload1", ftpUploadSettings);
		ToolExecution toolReturn = toolsClient.create(history, ftpUpload);

		assertEquals("unexpected number of outputs", 1, toolReturn.getOutputs().size());

		// dump out the IDs of the outputs as these as the outputs of the upload
		System.out.println("\t" + toolReturn.getOutputs().get(0).getId());

		// we can reuse the map just changing the file path to upload multiple
		// files in a row
		ftpUploadSettings.put("files_0|ftp_files", "folder/document002.txt");
		ftpUpload = new ToolInputs("upload1", ftpUploadSettings);
		toolReturn = toolsClient.create(history, ftpUpload);

		assertEquals("unexpected number of outputs", 1, toolReturn.getOutputs().size());

		// dump out the IDs of the outputs as these as the outputs of the upload
		System.out.println("\t" + toolReturn.getOutputs().get(0).getId());
	}

	@Test
	public void testUseStore() throws URISyntaxException, IOException, WorkflowException, InterruptedException {
		WorkflowServiceImpl workflowService = new WorkflowServiceImpl();
		workflowService.galaxyInstanceUrl="http://localhost:8080";
		workflowService.galaxyApiKey=
				"0403419ce354f40ff6503176c81ebbaf";
		workflowService.storeEndpoint = "http://localhost:8898";
		workflowService.galaxyFTPdir = "/home/mark/galaxy-ftp";
		workflowService.galaxyUserEmail = "m.a.greenwood@sheffield.ac.uk";

		StoreRESTClient storeClient = new StoreRESTClient(workflowService.storeEndpoint);

		Path archiveData = Paths.get(WorkflowServiceImplTest.class.getResource("/input/MultipleDocuments/").toURI());
		System.out.println(archiveData);

		String corpusId = uploadArchive(storeClient, archiveData);
		System.out.println(corpusId);

		String executionID = startWorkflow(workflowService, "Test1", corpusId);

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
