package eu.openminted.workflow.service;

import eu.openminted.workflow.api.*;

import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.stereotype.*;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

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
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInputs.ExistingHistory;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInputs.InputSourceType;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowInputs.WorkflowInput;
import com.github.jmchilton.blend4j.galaxy.beans.WorkflowOutputs;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

@RestController
@EnableAutoConfiguration
public class WorkflowServiceImpl implements WorkflowService {

    @RequestMapping("/")
    String home() {
        return "omtd-workflow-service";
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(WorkflowService.class, args);
    }

    public String execute(WorkflowJob workflowJob) throws WorkflowException {
	//get workflow from job, it's a Component instance so will then need to get it out of there
	//question is what does that object contain and where is the workflow file actually stored?

	//download the corpus from the OMTD-STORE using the REST client. This should get us a folder
	//but not sure the format of the contents has been fixed yet

	//run the workflow over the corups. can this be done in one shot or do we need to send each
	//document in turn. Might depend on how the workflow is written? 

	//return an ID that can be used to lookup the workflow later so you can get the status.
	//this probably means spawning a thread for the actual execution so we can monitor the
	//status of the Galaxy job as it runs
        return null;
    }

    public void cancel(String workflowExecutionId)  throws WorkflowException {

    }

    public void pause(String workflowExecutionId) throws WorkflowException {

    }

    public void resume(String workflowExecutionId) throws WorkflowException {

    }

    public ExecutionStatus getExecutionStatus(String workflowExecutionId) throws WorkflowException {
	//assuming execution has started this should be a case of polling galaxy as in the existing
	//demo code, to figure out where it's at, we just then need to wrap that as an ExecutionStatus
        return null;
    }

	/**
	 * Creates a new history to hold input and output files
	 * 
	 * @param instance
	 *            the Galaxy instance to use
	 * @param historyName
	 *            the name of the history you want to create
	 * @return the ID of the newly created history.
	 */
	static String createHistory(final GalaxyInstance instance, String name) {
		final History history = new History(name);
		final History newHistory = instance.getHistoriesClient().create(history);
		return newHistory.getId();
	}

	/**
	 * Upload the supplied file(s) into the specified history returning the
	 * resulting IDs
	 * 
	 * @param instance
	 *            the Galaxy instance to work with
	 * @param historyId
	 *            the history into which the file(s) should be uploaded
	 * @param files
	 *            the file(s) to upload
	 * @return the IDs of the uploaded files within the history instance
	 */
	static List<String> populateDatasets(final GalaxyInstance instance, final String historyId, File... files)
			throws InterruptedException {

		// TODO should we be doing this via the FileUploadRequest that takes
		// Iterable<File> instead?

		// create a list to hold the IDs
		final List<String> ids = Lists.newArrayListWithCapacity(files.length);

		for (final File file : files) {
			// for each file...

			// upload the file and...
			OutputDataset dataset = upload(instance, historyId, file);

			// and the ID to the list
			ids.add(dataset.getId());
		}

		// make sure everything has finished and the history is usable before...
		waitForHistory(instance.getHistoriesClient(), historyId);

		// returning the list of IDs
		return ids;
	}

	/**
	 * Many operations that modify the history don't complete instantly so we
	 * need to wait until the history is in a known ready stste. This method
	 * blocks until that state is achieved or it throws a RuntimeException if
	 * the history is in an unrecoverable error state.
	 * 
	 * @param client
	 *            the HistoryClient to use to access the history
	 * @param historyId
	 *            the ID of the history to check
	 */
	static void waitForHistory(final HistoriesClient client, final String historyId) throws InterruptedException {

		// a placeholder for the current details of the history
		HistoryDetails details = null;

		// wait until the history is in the ready state
		while (true) {
			details = client.showHistory(historyId);
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

		for (HistoryContents content : client.showHistoryContents(historyId)) {
			if (!content.getState().equals("ok")) {
				// if one of the history contents is in a failed state throw the
				// associated error
				HistoryContentsProvenance provenance = client.showProvenance(historyId, content.getId());
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

	/**
	 * Upload a single file into a given Galaxy history
	 * 
	 * @param galaxyInstance
	 *            the Galaxy instance to use
	 * @param historyId
	 *            the ID of the history to upload into
	 * @param file
	 *            the file to upload
	 * @return a dataset instance describing the uploaded file
	 */
	static OutputDataset upload(final GalaxyInstance galaxyInstance, final String historyId, final File file) {
		// create the upload request
		final ToolsClient.FileUploadRequest request = new ToolsClient.FileUploadRequest(historyId, file);

		// do the actual upload
		final ToolExecution execution = galaxyInstance.getToolsClient().upload(request);

		// return the dataset describing the uploaded file
		return execution.getOutputs().get(0);
	}

	/**
	 * Ensure that the specified workflow exists within the Galaxy instance
	 * 
	 * @param client
	 *            the workflow client used to talk to Galaxy
	 * @param workflowName
	 *            the name of the workflow; is both the workflow name and the
	 *            name of the workflow file should we need to upload it
	 * @return the ID of the workflow
	 */
	static String ensureHasWorkflow(WorkflowsClient client, final String workflowName) {

		for (Workflow workflow : client.getWorkflows()) {
			// for each workflow...

			if (workflow.getName().startsWith(workflowName)) {
				// if this is the workflow we are after then return it's ID
				return workflow.getId();
			}
		}

		try {
			// the workflow doesn't current exist in Galaxy so...

			// read the workflow definition in from a file
			String workflowContents = Resources
					.asCharSource(WorkflowServiceImpl.class.getResource(workflowName + ".ga"), Charsets.UTF_8).read();

			// upload the workflow to Galaxy
			Workflow workflow = client.importWorkflow(workflowContents);

			// return the ID of the uploaded workflow
			return workflow.getId();

		} catch (IOException ex) {
			// no idea what went wrong so just throw a exception
			throw new RuntimeException(ex);
		}

	}

	/**
	 * Given the label assigned to one of the workflows input this method
	 * returns the associated ID, or null if the label doesn't exist as an input
	 * in the specified workflow
	 * 
	 * @param workflowDetails
	 *            details of the workflow we are using
	 * @param inputLabel
	 *            the label of one of the workflows inputs
	 * @return the ID associated with the input label or null if such an input
	 *         does not exist within the workflow
	 */
	static String getWorkflowInputId(WorkflowDetails workflowDetails, String inputLabel) {
		// assume we won't find the label so get ready to return null
		String workflowInputId = null;

		for (final Map.Entry<String, WorkflowInputDefinition> inputEntry : workflowDetails.getInputs().entrySet()) {
			// for each input specified in the workflow...

			// get the label
			final String label = inputEntry.getValue().getLabel();

			if (label.equals(inputLabel)) {
				// if the label matches the one we are looking for then store
				// the ID
				workflowInputId = inputEntry.getKey();

				// no need to look any further
				break;
			}
		}

		// return the ID or null if we didn't find it
		return workflowInputId;
	}
}