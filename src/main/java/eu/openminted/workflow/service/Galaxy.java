package eu.openminted.workflow.service;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jmchilton.blend4j.galaxy.GalaxyInstance;
import com.github.jmchilton.blend4j.galaxy.GalaxyInstanceFactory;
import com.github.jmchilton.blend4j.galaxy.HistoriesClient;
import com.github.jmchilton.blend4j.galaxy.JobsClient;
import com.github.jmchilton.blend4j.galaxy.ToolsClient;
import com.github.jmchilton.blend4j.galaxy.WorkflowsClient;
import com.github.jmchilton.blend4j.galaxy.beans.Dataset;
import com.github.jmchilton.blend4j.galaxy.beans.History;
import com.github.jmchilton.blend4j.galaxy.beans.HistoryDetails;
import com.github.jmchilton.blend4j.galaxy.beans.JobDetails;
import com.github.jmchilton.blend4j.galaxy.beans.OutputDataset;
import com.github.jmchilton.blend4j.galaxy.beans.ToolExecution;
import com.github.jmchilton.blend4j.galaxy.beans.Workflow;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowDetails;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInputDefinition;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInputs;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInvocation;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInvocationInputs;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInvocationStep;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInvocationStepOutput;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowStepDefinition;
import com.github.jmchilton.blend4j.galaxy.beans.collection.request.CollectionDescription;
import com.github.jmchilton.blend4j.galaxy.beans.collection.request.HistoryDatasetElement;
import com.github.jmchilton.blend4j.galaxy.beans.collection.response.CollectionResponse;
import com.google.common.collect.Lists;

/**
 * @author ilsp
 *
 */
public class Galaxy {

	private static final Logger log = LoggerFactory.getLogger(Galaxy.class);

	private GalaxyInstance galaxyInstance = null;
	private WorkflowsClient workflowsClient;
	private HistoriesClient historiesClient;
	private JobsClient jobsClient;

	public Galaxy(String galaxyInstanceUrl, String galaxyApiKey) {
		galaxyInstance = GalaxyInstanceFactory.get(galaxyInstanceUrl, galaxyApiKey);
		workflowsClient = galaxyInstance.getWorkflowsClient();
		historiesClient = galaxyInstance.getHistoriesClient();
		jobsClient = galaxyInstance.getJobsClient();
	}

	public void runWorkflow(String workflowName, String workflowID, String historyID, List<String> inputIds,
			ArrayList<File> filesList, String outputPath) throws Exception {

		log.info("using history "+historyID);
		
		CollectionResponse collectionResponse = constructFileCollectionList(historyID, inputIds, filesList);
		log.info("Created file collection");

		log.info("---");
		
		WorkflowInvocation workflowInvocation = run(workflowName, workflowID, collectionResponse, historyID);

		String invocationID = workflowInvocation.getId();
		log.info("invocationID for " + workflowID + " " + invocationID);
		
		log.info("count tools");
		int count = workflowsClient.showWorkflow(workflowID).getSteps().size();
		log.info("tools counted:" + count);
					
		log.info("waitJobs");
		Map<String, WorkflowInvocationStepOutput> outputs = waitForInvocation(workflowID, invocationID, count);

		log.info("waitHistory:");
		waitAndMonitorHistory(historyID);
		// Jobs for this history have been completed
		// Also history is OK.
		// So, start downloading
		log.info("Starting download");
		downloadWorkflowOutputs(historyID, outputs, outputPath);
		log.info("Downloaded");
	}
	
	private WorkflowInvocation run(String workflowName, String workflowID, CollectionResponse collectionResponse,
			String historyID) {
		WorkflowInvocation workflowInvocation = null;

		WorkflowInputs.InputSourceType inputSource = WorkflowInputs.InputSourceType.HDCA;
		log.info(workflowName + "->" + workflowID);
		WorkflowDetails workflowDetails = workflowsClient.showWorkflow(workflowID);

		// String workflowInputId = getWorkflowInputId(workflowDetails,
		// "input_list");
		String workflowInputId = getWorkflowInputId(workflowDetails, "Input Dataset Collection");

		log.info("Configuring input");
		WorkflowInvocationInputs workflowInputs = new WorkflowInvocationInputs();
		workflowInputs.setDestination(new WorkflowInputs.ExistingHistory(historyID));
		workflowInputs.setWorkflowId(workflowID);
		workflowInputs.setInput(workflowInputId,
				new WorkflowInputs.WorkflowInput(collectionResponse.getId(), inputSource));

		setParameters(workflowInputs);
		printDetails(workflowInputs);
		
		
		log.info("Run workflow");
			
		workflowInvocation = invokeWorkflow(workflowInputs);
		log.info("Workflow started");

		return workflowInvocation;
	}
	
	private WorkflowInvocation invokeWorkflow(WorkflowInvocationInputs workflowInputs){
		log.info("invoke...workflow");
		WorkflowInvocation output = workflowsClient.invokeWorkflow(workflowInputs);
		return output;
	}
	
	private void waitAndMonitorHistory(String historyID) {
		// make sure the workflow has finished and the history is in
		// the "ok" state before proceeding any further
		try {
			long startTime = System.currentTimeMillis();
			log.info("Waiting history for results:");
			waitForHistory(historyID);
			long endTime = System.currentTimeMillis();
			long timeElapsed = (endTime - startTime);
			log.info("Waited history for results:" + timeElapsed + " " + timeElapsed / 1000);

		} catch (InterruptedException e) {
			// hmmmm that will mess things up
			log.error("Interrupted waiting for a valid Galaxy history", e);
			// status.put(workflowExecutionId, new ExecutionStatus(e));
			// return output;
		}
	}

	private CollectionResponse constructFileCollectionList(String historyId, List<String> inputIds, List<File> files) {
		HistoriesClient historiesClient = galaxyInstance.getHistoriesClient();

		CollectionDescription collectionDescription = new CollectionDescription();
		collectionDescription.setCollectionType("list");
		collectionDescription.setName(historyId + "collection");
		// collectionDescription.setType("pdf");

		int i = 0;
		for (String inputId : inputIds) {
			HistoryDatasetElement element = new HistoryDatasetElement();
			element.setId(inputId);
			element.setName(files.get(i).getName());
			i++;
			collectionDescription.addDatasetElement(element);
		}

		return historiesClient.createDatasetCollection(historyId, collectionDescription);
	}

	public String createHistory(String name) {
		final History history = new History(name);
		final History newHistory = galaxyInstance.getHistoriesClient().create(history);
		return newHistory.getId();
	}

	public String createHistory() {
		return createHistory(Galaxy.class.getName() + "-" + new Date());
	}

	public String ensureHasWorkflow(final String workflowName) {
		String workflowId = null;
		for (Workflow workflow : workflowsClient.getWorkflows()) {
			log.info(workflow.getName() + " " + workflowName);
			if (workflow.getName().startsWith(workflowName)) {
				workflowId = workflow.getId();
				break;
			}
		}
		return workflowId;
	}

	public List<String> populateHistory(final String historyId, final List<File> files) throws InterruptedException {
		final List<String> ids = Lists.newArrayListWithCapacity(files.size());
		for (final File file : files) {
			OutputDataset dataset = uploadFileToHistory(historyId, file);
			ids.add(dataset.getId());
		}
		waitForHistory(historyId);
		return ids;
	}

	public OutputDataset uploadFileToHistory(final String historyId, final File file) {
		final ToolsClient.FileUploadRequest request = new ToolsClient.FileUploadRequest(historyId, file);
		final ToolExecution execution = galaxyInstance.getToolsClient().upload(request);
		return execution.getOutputs().get(0);
	}
	
	public void waitForHistory(final String historyId) throws InterruptedException {

		// a placeholder for the current details of the history
		HistoryDetails details = null;

		// wait until the history is in the ready state
		while (true) {
			details = historiesClient.showHistory(historyId);
			if (details.isReady()) {
				break;
			}

			// don't hammer the galaxy instance to heavily
			Thread.sleep(200L);
		}
		
		// if the history is in an ok state then we can return
		String state = null;
		while (true) {
			details = historiesClient.showHistory(historyId);
			state = details.getState();
			if (state.equals("ok")) {
				System.out.println(details.getStateIds());
				return;
			}				
			else {
				System.out.println("History state: " + state);
				//TODO if we fail we need to return rather than sitting in an infinite loop
			}

			Thread.sleep(1000L);

		}
/*
		for (HistoryContents content : historiesClient.showHistoryContents(historyId)) {
			if (content != null && content.getState() != null && !content.getState().equals("ok")) {
				// if one of the history contents is in a failed state throw the
				// associated error
				HistoryContentsProvenance provenance = historiesClient.showProvenance(historyId, content.getId());
				final String standardError = provenance.getStandardError().replace("\n", "");
				final String message = "History no longer running, but not in 'ok' state. Found content in error with standard error "
						+ standardError;
				throw new RuntimeException(message);
			}
		}

		// throw a slightly more generic error message simply reporting the
		// state of the entire history
		throw new RuntimeException("History no longer running, but not in 'ok' state. State is - " + state);*/

	}
	
	public Map<String,WorkflowInvocationStepOutput> waitForInvocation(String workflowId, String invocationId, int stepCount) throws InterruptedException {
		
		WorkflowInvocation invocation = null;
		
		//wait for all steps to have been added to the invocation
		while (true) {
			
			try{
				invocation = workflowsClient.showInvocation(workflowId, invocationId);
				
				if(invocation == null){
					log.info("invocation is null..returning");
					//TODO should probably be an exception instead
					return null;
				}
				
				if (invocation.getWorkflowSteps().size() == stepCount) {
					break;
				}
			}catch(Exception e){
				e.printStackTrace();
				log.info(e.getMessage());
			}
			
			Thread.sleep(200);
		}
		
		while (true) {
			try{
				invocation = workflowsClient.showInvocation(workflowId, invocationId);
				
				if(invocation == null){
					log.info("invocation is null..returning");
					return null;
				}
				
				log.info("workflow steps size:" + invocation.getWorkflowSteps().size());
				WorkflowInvocationStep step = invocation.getWorkflowSteps().get(stepCount-1);
				
				if(step!= null){
					log.info("Step state:" + step.getState());
					String jobId = step.getJobId();
					
					JobDetails jobDetails = jobsClient.showJob(jobId);
					log.info("JOB DETAILS: "+jobDetails.getState()+"|"+jobDetails.getExitCode());
				}
				
				if (step!= null && step.getState()!= null && step.getState().equals("ok")) {
					
					while (true) {
						String jobId = step.getJobId();
						JobDetails jobDetails = jobsClient.showJob(jobId);
						log.info("JOB DETAILS: "+jobDetails.getState()+"|"+jobDetails.getExitCode());
						
						if (jobDetails.getState().equals("ok") && jobDetails.getExitCode() != null) {
							
							while (true) {
								step = workflowsClient.showInvocationStep(workflowId, invocationId, step.getId());
								System.out.println("Step outputs: "+step.getOutputs());
								if (step.getOutputs() != null && step.getOutputs().size() > 0)
									return step.getOutputs();							
							}
						}
						
					}
					
				}
			}catch(Exception e){
				e.printStackTrace();
				log.info(e.getMessage());
			}
			
			Thread.sleep(200);
		}
	}

	private String getWorkflowInputId(WorkflowDetails workflowDetails, String inputLabel) {
		String workflowInputId = null;

		for (final Map.Entry<String, WorkflowInputDefinition> inputEntry : workflowDetails.getInputs().entrySet()) {
			final String label = inputEntry.getValue().getLabel();
			log.info("label: " + label);
			if (label.equals(inputLabel)) {
				log.info("equal label: " + inputLabel);
				workflowInputId = inputEntry.getKey();
				log.info("workflowInputIdl: " + workflowInputId);
			}
		}

		return workflowInputId;
	}

	private void setParameters(WorkflowInputs inputs) {
		final WorkflowDetails workflowDetails = workflowsClient.showWorkflow(inputs.getWorkflowId());
		// workflowDetails.getInputs();

		for (final Map.Entry<String, WorkflowStepDefinition> entry : workflowDetails.getSteps().entrySet()) {
			final String stepId = entry.getKey();

			if (stepId.equalsIgnoreCase("1")) {
				// inputs.setToolParameter(stepId, "Workflow ID",
				// "eu.openminted.simplewokflows.dkpro.PipelinePDFToXMI");
				// inputs.setStepParameter(stepId, "Workflow ID",
				// "eu.openminted.simplewokflows.dkpro.PipelinePDFToXMI");
				/// log.info(stepId + " parameter has been set");
			}
		}
	}

	private void printDetails(WorkflowInputs inputs) {
		final WorkflowDetails workflowDetails = workflowsClient.showWorkflow(inputs.getWorkflowId());
		// workflowDetails.getInputs();

		for (final Map.Entry<String, WorkflowStepDefinition> entry : workflowDetails.getSteps().entrySet()) {
			final String stepId = entry.getKey();
			final WorkflowStepDefinition stepDef = entry.getValue();

			log.info(stepId + " " + stepDef.getType()
					+ " " /* stepDef.toString() */);

			for (final Map.Entry<String, WorkflowStepDefinition.WorkflowStepOutput> stepInput : stepDef.getInputSteps()
					.entrySet()) {
				log.info("Parameter " + stepInput.getKey() + "-" + stepInput.getValue() + "-"
						+ /* stepInput.toString() */ "\n");
			}

		}
	}
	
	private void downloadWorkflowOutputs(String historyId, Map<String,WorkflowInvocationStepOutput> outputs, String path){
		
		// download this output into the local file
		try {

			for (Map.Entry<String, WorkflowInvocationStepOutput> output : outputs.entrySet()) {
				
				log.info(output.getKey()+"|"+output.getValue().getType()+"|"+output.getValue().getId());
				
				if (output.getValue().getType().equals(WorkflowInputs.InputSourceType.HDA)) {
					Dataset dataset = historiesClient.showDataset(historyId, output.getValue().getId());
					File f = new File(path);
					if (!f.exists()) {
						f.mkdirs();
					}
					File outputFile = new File(path + dataset.getName());
					log.info("downloading "+dataset.getName()+" to " +outputFile.getAbsolutePath());
					historiesClient.downloadDataset(historyId, output.getValue().getId(), outputFile);				}
			}

		} catch (Exception e) {
			// if we can't download the file then we have a
			// problem....
			log.info("Unable to download result from Galaxy history", e);
			// status.put(workflowExecutionId, new ExecutionStatus(e));
			// return;
		}	
	}
}
