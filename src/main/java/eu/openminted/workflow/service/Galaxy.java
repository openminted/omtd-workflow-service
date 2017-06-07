package eu.openminted.workflow.service;

import java.io.File;
import java.io.IOException;
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
import com.github.jmchilton.blend4j.galaxy.ToolsClient;
import com.github.jmchilton.blend4j.galaxy.WorkflowsClient;
import com.github.jmchilton.blend4j.galaxy.beans.History;
import com.github.jmchilton.blend4j.galaxy.beans.HistoryContents;
import com.github.jmchilton.blend4j.galaxy.beans.HistoryContentsProvenance;
import com.github.jmchilton.blend4j.galaxy.beans.HistoryDetails;
import com.github.jmchilton.blend4j.galaxy.beans.OutputDataset;
import com.github.jmchilton.blend4j.galaxy.beans.ToolExecution;
import com.github.jmchilton.blend4j.galaxy.beans.Workflow;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowDetails;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInputDefinition;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInputs;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowOutputs;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowStepDefinition;
import com.github.jmchilton.blend4j.galaxy.beans.collection.request.CollectionDescription;
import com.github.jmchilton.blend4j.galaxy.beans.collection.request.HistoryDatasetElement;
import com.github.jmchilton.blend4j.galaxy.beans.collection.response.CollectionElementResponse;
import com.github.jmchilton.blend4j.galaxy.beans.collection.response.CollectionResponse;
import com.google.common.collect.Lists;

import eu.openminted.workflow.api.ExecutionStatus;

/**
 * @author ilsp
 *
 */
public class Galaxy {

	private static final Logger log = LoggerFactory.getLogger(Galaxy.class);

	private GalaxyInstance galaxyInstance = null;
	private WorkflowsClient workflowsClient;
	private HistoriesClient historiesClient;

	public Galaxy(String galaxyInstanceUrl, String galaxyApiKey) {
		galaxyInstance = GalaxyInstanceFactory.get(galaxyInstanceUrl, galaxyApiKey);
		workflowsClient = galaxyInstance.getWorkflowsClient();
		historiesClient = galaxyInstance.getHistoriesClient();
	}
	
	public void runWorkflow(String inputData, String workflowId, String outputPAth) {

		String hasWorkflow = ensureHasWorkflow(workflowId);
		if (hasWorkflow != null) {

			try {
				String historyID = createHistory();
				File dir = new File(inputData);
				File PDFs[] = dir.listFiles();
				ArrayList<File> filesList = new ArrayList<File>(Arrays.asList(PDFs));

				log.info("Started history population");
				List<String> inputIds = populateHistory(historyID, filesList);
				log.info("Populated history");

				this.runWorkflow(workflowId, hasWorkflow, historyID, inputIds, filesList, outputPAth);
			} catch (Exception e) {
				log.info(e.getMessage());
			}

		} else {
			log.info("Workflow " + workflowId + " does not exist");
		}
	}

	public void runWorkflow(String workflowId, String hasWorkflow, String historyID, List<String> inputIds,
			ArrayList<File> filesList, String outputPAth) {
		try {
			CollectionResponse collectionResponse = constructFileCollectionList(historyID, inputIds, filesList);
			log.info("Created file collection");

			WorkflowOutputs output = run(workflowId, hasWorkflow, collectionResponse, historyID);

			log.info("Download");
			download(output, outputPAth);
			log.info("Downloaded");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private WorkflowOutputs run(String workflowId, String hasWorkflow, CollectionResponse collectionResponse,
			String historyID) {
		WorkflowOutputs output = null;

		WorkflowInputs.InputSourceType inputSource = WorkflowInputs.InputSourceType.HDCA;
		log.info(workflowId + "->" + hasWorkflow);
		WorkflowDetails workflowDetails = workflowsClient.showWorkflow(hasWorkflow);

		// String workflowInputId = getWorkflowInputId(workflowDetails,
		// "input_list");
		String workflowInputId = getWorkflowInputId(workflowDetails, "Input Dataset Collection");

		WorkflowInputs workflowInputs = new WorkflowInputs();
		workflowInputs.setDestination(new WorkflowInputs.ExistingHistory(historyID));
		workflowInputs.setWorkflowId(hasWorkflow);
		workflowInputs.setInput(workflowInputId,
				new WorkflowInputs.WorkflowInput(collectionResponse.getId(), inputSource));

		setParameters(workflowInputs);
		printDetails(workflowInputs);
		log.info("Run workflow");
		output = workflowsClient.runWorkflow(workflowInputs);
		log.info("Workflow started");

		log.info("Waiting");
		// make sure the workflow has finished and the history is in
		// the "ok" state before proceeding any further
		try {
			// waitForHistory(output.getHistoryId());
			waitForHistory(historyID);
		} catch (InterruptedException e) {
			// hmmmm that will mess things up
			log.error("Interrupted waiting for a valid Galaxy history", e);
			// status.put(workflowExecutionId, new ExecutionStatus(e));
			return output;
		}

		return output;
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

	private String getWorkflowInputId(WorkflowDetails workflowDetails, String inputLabel) {
		String workflowInputId = null;

		for (final Map.Entry<String, WorkflowInputDefinition> inputEntry : workflowDetails.getInputs().entrySet()) {
			final String label = inputEntry.getValue().getLabel();
			log.info("label: " + label);
			if (label.equals(inputLabel)) {
				workflowInputId = inputEntry.getKey();
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
				log.info(stepId + " parameter has been set");
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

	private void download(WorkflowOutputs output, String path) {

		try {
			Thread.sleep(120000L);
		} catch (Exception e) {

		}

		for (final String outputId : output.getOutputIds()) {
			System.out.println("  Workflow Output ID " + outputId);
		}

		String outputId = output.getOutputIds().get(output.getOutputIds().size() - 1);
		log.info("outputId:" + outputId);

		// download this output into the local file
		try {

			List<HistoryContents> hc = historiesClient.showHistoryContents(output.getHistoryId());
			for (final HistoryContents element : hc) {
				log.info(element.getId() + "|" + element.getName() + "|" + element.getHistoryContentType());

				File f = new File(path);
				if (!f.exists()) {
					f.mkdirs();
				}
				File outputFile = new File(path + element.getName());

				if (element.getHistoryContentType().equalsIgnoreCase("dataset")) {
					log.info("Downloading dataset to " + outputFile.getAbsolutePath());
					historiesClient.downloadDataset(output.getHistoryId(), element.getId(), outputFile);
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
			log.error("Unable to download result from Galaxy history", e);
			// status.put(workflowExecutionId, new ExecutionStatus(e));
			return;
		}

	}

}
