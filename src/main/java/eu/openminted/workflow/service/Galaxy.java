package eu.openminted.workflow.service;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
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
import com.github.jmchilton.blend4j.galaxy.beans.History;
import com.github.jmchilton.blend4j.galaxy.beans.HistoryContents;
import com.github.jmchilton.blend4j.galaxy.beans.HistoryContentsProvenance;
import com.github.jmchilton.blend4j.galaxy.beans.HistoryDetails;
import com.github.jmchilton.blend4j.galaxy.beans.Job;
import com.github.jmchilton.blend4j.galaxy.beans.JobDetails;
import com.github.jmchilton.blend4j.galaxy.beans.JobInputOutput;
import com.github.jmchilton.blend4j.galaxy.beans.OutputDataset;
import com.github.jmchilton.blend4j.galaxy.beans.ToolExecution;
import com.github.jmchilton.blend4j.galaxy.beans.Workflow;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowDetails;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInputDefinition;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInputs;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInvocation;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInvocation.Step;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInvocationInputs;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowOutputs;
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

	private String scriptsPath;
	
	
	public String getScriptsPath() {
		return scriptsPath;
	}

	public void setScriptsPath(String scriptsPath) {
		this.scriptsPath = scriptsPath;
	}

	public Galaxy(String galaxyInstanceUrl, String galaxyApiKey) {
		galaxyInstance = GalaxyInstanceFactory.get(galaxyInstanceUrl, galaxyApiKey);
		workflowsClient = galaxyInstance.getWorkflowsClient();
		historiesClient = galaxyInstance.getHistoriesClient();
		jobsClient = galaxyInstance.getJobsClient();
	}

	public void runWorkflow(String inputData, String workflowName, String outputPAth) {

		String workflowID = ensureHasWorkflow(workflowName);
		if (workflowID != null) {

			try {
				String historyID = createHistory();
				File dir = new File(inputData);
				File PDFs[] = dir.listFiles();
				ArrayList<File> filesList = new ArrayList<File>(Arrays.asList(PDFs));

				log.info("Started history population");
				List<String> inputIds = populateHistory(historyID, filesList);
				log.info("Populated history");

				this.runWorkflow(workflowName, workflowID, historyID, inputIds, filesList, outputPAth);
			} catch (Exception e) {
				log.info(e.getMessage());
			}

		} else {
			log.info("Workflow " + workflowName + " does not exist");
		}
	}

	public void runWorkflow(String workflowName, String workflowID, String historyID, List<String> inputIds,
			ArrayList<File> filesList, String outputPAth) throws Exception {

		CollectionResponse collectionResponse = constructFileCollectionList(historyID, inputIds, filesList);
		log.info("Created file collection");

		log.info("---");
		//WorkflowOutputs workflowOutputs = run(workflowName, workflowID, collectionResponse, historyID);
		WorkflowInvocation workflowInvocation = run(workflowName, workflowID, collectionResponse, historyID);

		String invocationID = workflowInvocation.getId();
		log.info("invocationID for " + workflowID + " " + invocationID);
		
		log.info("count tools");
		//int count = countTools(workflowID);
		int count = workflowsClient.showWorkflow(workflowID).getSteps().size();
		log.info("tools counted:" + count);
				
		//log.info("waitUntilHistoryIsReady");
		//waitUntilHistoryIsReady(historyID);
		
		log.info("waitJobs");
		//waitJobs(historyID, count, workflowInvocation.getWorkflowId(), invocationID);
		waitForInvocation(workflowID, invocationID, count);

		log.info("waitHistory:");
		waitAndMonitorHistory(historyID);

		//waitBeforeStartDown();
		
		// Jobs for this history have been completed
		// Also history is OK.
		// So, start downloading
		log.info("Starting download");
		//download(workflowOutputs, outputPAth);
		downloadHistoryContents(historyID, outputPAth);
		log.info("Downloaded");
	}

	private void  waitBeforeStartDown() throws Exception{
		log.info("waitBeforeStartDown");
		Thread.currentThread().sleep(10000);
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
		
		// output = workflowsClient.runWorkflow(workflowInputs);
		// runWorkflowPy(workflowID, historyID, collectionResponse.getId());	
		workflowInvocation = invokeWorkflow(workflowInputs);
		log.info("Workflow started");

		// waitForHistory2(historyID);

		return workflowInvocation;
	}
	
	private WorkflowInvocation invokeWorkflow(WorkflowInvocationInputs workflowInputs){
		log.info("invoke...workflow");
		WorkflowInvocation output = workflowsClient.invokeWorkflow(workflowInputs);
		return output;
	}
	
	private void runWorkflowPy(String workflowID, String historyID, String collectionID){
		try {
			Runtime rt = Runtime.getRuntime();
			// galaxyEnpoint, galaxyAPIKey, wid, inputSource, collectionID, historyID
			String cmd = "/usr/bin/python " + this.scriptsPath + "invoke.py " + 
					this.galaxyInstance.getGalaxyUrl() + " " + 
					this.galaxyInstance.getApiKey() + " " + 
					workflowID + " " +
					"hdca " +  
					collectionID + " " + 
					historyID;
			log.info("cmd:" + cmd);		
			Process proc = rt.exec(cmd);
			proc.waitFor();
			int exitVal = proc.exitValue();
			
			log.info("Process exitValue for invoke workflow: " + exitVal);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	
	}

	// ------
	// https://github.com/CTMM-TraIT/trait_workflow_runner/blob/c2dffbfcfc18a258dac6229e06e56586dadb0990/src/main/java/nl/vumc/biomedbridges/galaxy/GalaxyWorkflowEngine.java
		
	private void waitUntilHistoryIsReady(final String historyId) throws InterruptedException{
		boolean finished = false;
		
	    while (!finished) {
            log.info("- Now waiting for seconds...");
            Thread.sleep(5000);
            finished = isHistoryReady(historyId);
        }
   
    }
	
	private boolean isHistoryReady(final String historyId) {
		final HistoryDetails historyDetails = historiesClient.showHistory(historyId);
	    // If the input/output file count is known, it could be checked too:
	    //                       historyDetails.getStateIds().get(STATE_OK).size() == [n]
	    final boolean finished = historyDetails.getStateIds().get("running").size() == 0
	                             && historyDetails.getStateIds().get("queued").size() == 0;
	    log.info("finished: " + finished);
	    log.info("History state IDs: {}.", historyDetails.getStateIds());
	    return finished;
	}
	// ------
	
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

	private void waitJobs(String historyID, int count, String workflowID, String invocationID) {

		while (!jobsForHistoryAreCompleted(historyID, count, workflowID, invocationID)) {

			try {
				log.info("Sleep");
				Thread.sleep(5000L);
			} catch (Exception e) {
				log.info("Sleep issue");
			}
		}
	}

	private boolean jobsForHistoryAreCompleted(String historyID, int count,  String workflowID, String invocationID) {
		boolean completed = true;
		
		List<Job> jobs = jobsClient.getJobsForHistory(historyID);
		log.info("Jobs for history" + historyID + " are " + jobs.size());

		log.info("workflowInvocation" + invocationID);
		WorkflowInvocation wi = workflowsClient.showInvocation(workflowID, invocationID);
		log.info("workflowInvocation" + wi.getState() + " " + wi.getUpdateTime());
		
		int toolsCount = 0;
		for (Job job : jobs) {

			log.info(jobToString(job));

			JobDetails jobDetails = jobsClient.showJob(job.getId());
			Integer exitCode = jobDetails.getExitCode();

			log.info("Exit code:" + exitCode);
			Iterator<String> it = jobDetails.getOutputs().keySet().iterator();
			while (it.hasNext()) {
				String key = it.next();
				JobInputOutput jIO = jobDetails.getOutputs().get(key);
				log.info("JobInputOutput:" + jIO.getSource());
			}

			/*
			 * if(! (job.getState().equalsIgnoreCase("ok") ||
			 * job.getState().equalsIgnoreCase("error")) ){ completed = false;
			 * break; }
			 */

			if (!job.getToolId().startsWith("upload")){				
				toolsCount++;
				log.info(job.getToolId() + "***" + toolsCount);
			}
				
			if (exitCode == null) {
				completed = false;
				break;
			}
		}

		return completed && (toolsCount == count);
		//&& wi.getState().equalsIgnoreCase("ready");
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

	/*
	 * private void waitForHistory(final String historyId) throws
	 * InterruptedException { HistoryDetails details = null; while (true) {
	 * details = historiesClient.showHistory(historyId); if (details.isReady())
	 * { break; } } final String state = details.getState(); if
	 * (!state.equals("ok")) { for (HistoryContents content :
	 * historiesClient.showHistoryContents(historyId)) { if
	 * (!content.getState().equals("ok")) { HistoryContentsProvenance provenance
	 * = historiesClient.showProvenance(historyId, content.getId()); final
	 * String standardError = provenance.getStandardError().replace("\n", "");
	 * final String message =
	 * "History no longer running, but not in 'ok' state. Found content in error with standard error "
	 * + standardError; throw new RuntimeException(message); } } final String
	 * message = "History no longer running, but not in 'ok' state. State is - "
	 * + state; throw new RuntimeException(message); } Thread.sleep(200L); }
	 */

	private boolean allHistoryContentAreOK(String historyId){
	int all = 0;
	int Ok = 0;
		for (HistoryContents content : historiesClient.showHistoryContents(historyId)) {
			all++;
			if (!content.getState().equals("ok")) {
				Ok++;
			}
		}
		
		return (all == Ok);
	}

	
	private void waitForHistory(final String historyId) throws InterruptedException {

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

		/*
		while (true) {
			boolean allOK = allHistoryContentAreOK(historyId);
			if (allOK) {
				break;
			}

			// don't hammer the galaxy instance to heavily
			Thread.sleep(200L);
		}*/
		
		// get the state of the history
		final String state = details.getState();
		Thread.sleep(200L);

		// if the history is in an ok state then we can return
		if (state.equals("ok"))
			return;

		for (HistoryContents content : historiesClient.showHistoryContents(historyId)) {
			if (!content.getState().equals("ok")) {
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
		throw new RuntimeException("History no longer running, but not in 'ok' state. State is - " + state);

	}
	
	public void waitForInvocation(String workflowId, String invocationId, int stepCount) throws InterruptedException {
		
		WorkflowInvocation invocation = null;
		
		//wait for all steps to have been added to the invocation
		while (true) {
			
			try{
				invocation = workflowsClient.showInvocation(workflowId, invocationId);
				
				if(invocation == null){
					log.info("invocation is null..returning");
					return;
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
					return;
				}
				
				log.info("workflow steps size:" + invocation.getWorkflowSteps().size());
				Step step = invocation.getWorkflowSteps().get(stepCount-1);
				
				if(step!= null){
					log.info("Step state:" + step.getState());					
				}
				
				if (step!= null && step.getState()!= null && step.getState().equals("ok")) {
					break;
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

	private int countTools(String wid) {
		int count = 0;
		final WorkflowDetails workflowDetails = workflowsClient.showWorkflow(wid);
		// workflowDetails.getInputs();

		for (final Map.Entry<String, WorkflowStepDefinition> entry : workflowDetails.getSteps().entrySet()) {
			final String stepId = entry.getKey();
			final WorkflowStepDefinition stepDef = entry.getValue();
			
			if(stepDef.getType().equalsIgnoreCase("tool")){
				count++;
			}
		}
		
		return count;
	}
	private void download(WorkflowOutputs output, String path) {

		for (final String outputId : output.getOutputIds()) {
			log.info("Workflow Output ID " + outputId);
		}

		int outIndex = output.getOutputIds().size() - 1;
		String outputId = null;

		if (outIndex >= 0) {
			outputId = output.getOutputIds().get(outIndex);
			log.info("outputId:" + outputId);
		} else {
			log.info("outIndex = " + outIndex);
			log.info("no intermediate results");
			outIndex = 0;
		}
		
		downloadHistoryContents(output.getHistoryId(), path);
	}

	private void downloadHistoryContents(String historyID, String path){
		
		// download this output into the local file
		try {

			List<HistoryContents> hc = historiesClient.showHistoryContents(historyID);
			for (final HistoryContents element : hc) {
				log.info(element.getId() + "|" + element.getName() + "|" + element.getHistoryContentType());

				File f = new File(path);
				if (!f.exists()) {
					f.mkdirs();
				}
				File outputFile = new File(path + element.getName());

				if (element.getHistoryContentType().equalsIgnoreCase("dataset")) {
					log.info("Downloading dataset to " + outputFile.getAbsolutePath());
					historiesClient.downloadDataset(historyID, element.getId(), outputFile);
				}
				

				/*
				 * if("Produce XML files".equalsIgnoreCase(element.getName())){
				 * 
				 * CollectionResponse cr =
				 * historiesClient.showDatasetCollection(output.getHistoryId(),
				 * element.getId());
				 * 
				 * log.info("size:" + cr.getElements().size() );
				 * 
				 * Iterator<CollectionElementResponse> it =
				 * cr.getElements().iterator(); while(it.hasNext()){
				 * CollectionElementResponse resp = it.next(); log.info("--- " +
				 * resp.getId() + " " + resp.getElementType()); File outputFile
				 * = new File(path, resp.getId()); outputFile.mkdirs();
				 * historiesClient.downloadDataset(output.getHistoryId(),
				 * resp.getId(), outputFile); } //cr. }
				 */
			}

		} catch (Exception e) {
			// if we can't download the file then we have a
			// problem....
			log.info("Unable to download result from Galaxy history", e);
			// status.put(workflowExecutionId, new ExecutionStatus(e));
			// return;
		}	
	}
	
	private String jobToString(Job job) {
		String ret = "job[ " + "state:" + job.getState() + ", " + "toolID:" + job.getToolId() + ", " + "created:"
				+ job.getCreated() + ", " + "updated:" + job.getUpdated() + ", " + "]";
		return ret;
	}
}
