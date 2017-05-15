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
import eu.openminted.workflow.api.WorkflowException;
import eu.openminted.workflow.api.WorkflowJob;
import junit.framework.TestCase;

public class WorkflowServiceImplTest extends TestCase {

	@Test
	public void testSingleDocumentANNIEWorkflow() throws WorkflowException {
		WorkflowServiceImpl workflowService = new WorkflowServiceImpl();
		workflowService.galaxyInstanceUrl = "http://localhost:8899";
		workflowService.galaxyApiKey = "4454f8849b3d30e1a6551727f871dbd7";

		// This can't possibly be the right way to set the ID of the workflow we
		// want to run. Who codes something five levels deep just to get a
		// single ID, especially as you end up at a list!
		ResourceIdentifier workflowId = new ResourceIdentifier();
		workflowId.setValue("ANNIE");

		IdentificationInfo workflowIdInfo = new IdentificationInfo();
		workflowIdInfo.setIdentifiers(Arrays.asList(new ResourceIdentifier[] { workflowId }));

		ComponentInfo workflowInfo = new ComponentInfo();
		workflowInfo.setIdentificationInfo(workflowIdInfo);

		Component workflow = new Component();
		workflow.setComponentInfo(workflowInfo);

		WorkflowJob workflowJob = new WorkflowJob();
		workflowJob.setWorkflow(workflow);
		
		File folder = toFile(this.getClass().getResource("/input/SingleDocument/"));
		
		System.out.println(folder.getAbsolutePath());
		
		
		workflowJob.setCorpusId(folder.getAbsolutePath());

		System.out.println("about to execute");
		workflowService.execute(workflowJob);
	}
	
	@Test
	public void testMultipleDocumentANNIEWorkflow() throws WorkflowException {
		WorkflowServiceImpl workflowService = new WorkflowServiceImpl();
		workflowService.galaxyInstanceUrl = "http://localhost:8899";
		workflowService.galaxyApiKey = "4454f8849b3d30e1a6551727f871dbd7";

		// This can't possibly be the right way to set the ID of the workflow we
		// want to run. Who codes something five levels deep just to get a
		// single ID, especially as you end up at a list!
		ResourceIdentifier workflowId = new ResourceIdentifier();
		workflowId.setValue("ANNIE");

		IdentificationInfo workflowIdInfo = new IdentificationInfo();
		workflowIdInfo.setIdentifiers(Arrays.asList(new ResourceIdentifier[] { workflowId }));

		ComponentInfo workflowInfo = new ComponentInfo();
		workflowInfo.setIdentificationInfo(workflowIdInfo);

		Component workflow = new Component();
		workflow.setComponentInfo(workflowInfo);

		WorkflowJob workflowJob = new WorkflowJob();
		workflowJob.setWorkflow(workflow);
		
		File folder = toFile(this.getClass().getResource("/input/MultipleDocuments/"));
		
		System.out.println(folder.getAbsolutePath());
		
		
		workflowJob.setCorpusId(folder.getAbsolutePath());

		System.out.println("about to execute");
		workflowService.execute(workflowJob);
	}
	
	private static File toFile(URL url) {
		try {
			return new File(url.toURI());
		}
		catch (URISyntaxException e) {
			return new File(url.getPath());
		}
	}
}
