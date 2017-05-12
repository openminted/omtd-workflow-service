package eu.openminted.workflow.service;

import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.util.ReflectionTestUtils;

import eu.openminted.registry.domain.Component;
import eu.openminted.registry.domain.ComponentInfo;
import eu.openminted.registry.domain.IdentificationInfo;
import eu.openminted.registry.domain.ResourceIdentifier;
import eu.openminted.workflow.api.WorkflowException;
import eu.openminted.workflow.api.WorkflowJob;
import eu.openminted.workflow.api.WorkflowService;
import junit.framework.TestCase;

public class WorkflowServiceImplTest extends TestCase {
	
	@Test
	public void testSingleDocumentANNIEWorkflow() throws WorkflowException {
		WorkflowServiceImpl workflowService = new WorkflowServiceImpl();
		workflowService.galaxyInstanceUrl = "http://localhost:8899";
		workflowService.galaxyApiKey = "4454f8849b3d30e1a6551727f871dbd7";
		System.out.println("running");
				
		ResourceIdentifier workflowId = new ResourceIdentifier();
		workflowId.setValue("ANNIE");
		
		IdentificationInfo workflowIdInfo = new IdentificationInfo();
		workflowIdInfo.setIdentifiers(Arrays.asList(new ResourceIdentifier[]{workflowId}));
		
		ComponentInfo workflowInfo = new ComponentInfo();
		workflowInfo.setIdentificationInfo(workflowIdInfo);
		
		Component workflow = new Component();
		workflow.setComponentInfo(workflowInfo);

		WorkflowJob workflowJob = new WorkflowJob();
		workflowJob.setWorkflow(workflow);
		workflowJob.setCorpusId("SingleDocument");

		System.out.println("about to execute");
		workflowService.execute(workflowJob);
	}
}
