package eu.openminted.workflow.service;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

import eu.openminted.store.common.StoreResponse;
import eu.openminted.store.restclient.StoreRESTClient;
import eu.openminted.workflow.api.ExecutionStatus;
import eu.openminted.workflow.api.ExecutionStatus.Status;
import eu.openminted.workflow.api.WorkflowException;
import eu.openminted.workflow.api.WorkflowJob;
import eu.openminted.workflow.api.WorkflowService;

@RestController
@EnableAutoConfiguration
public class WorkflowServiceImpl implements WorkflowService {

	private static Logger log = Logger.getLogger(WorkflowServiceImpl.class);

	private static Map<String, ExecutionStatus> status = new HashMap<String, ExecutionStatus>();

	// these should probably both be set via injection
	@Value("${galaxy.url}")
	String galaxyInstanceUrl;

	@Value("${galaxy.apiKey}")
	String galaxyApiKey;

	@Value("${store.endpoint}")
	private String storeEndpoint;

	@RequestMapping("/")
	String home() {
		return "omtd-workflow-service for <a href=\"" + galaxyInstanceUrl + "\">galaxy</a>";
	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(WorkflowServiceImpl.class, args);
	}

	@SuppressWarnings("unused")
	@Override
	public String execute(WorkflowJob workflowJob) throws WorkflowException {

		final String workflowExecutionId = UUID.randomUUID().toString();
		status.put(workflowExecutionId, new ExecutionStatus(Status.PENDING));

		log.debug("Starting workflow execution " + workflowExecutionId + " using Galaxy instance at "
				+ galaxyInstanceUrl);

		Runnable runner = new Runnable() {

			public void run() {
				// get a handle on the Galaxy instance we want to talk to
				GalaxyInstance instance = GalaxyInstanceFactory.get(galaxyInstanceUrl, galaxyApiKey);

				StoreRESTClient storeClient = new StoreRESTClient(storeEndpoint);

				/**
				 * get workflow from job, it's a Component instance so will then
				 * need to get it out of there. question is what does that
				 * object contain and where is the workflow file actually
				 * stored?
				 **/

				// get clients for access to the workflows and histories
				WorkflowsClient client = instance.getWorkflowsClient();

				// This can't possible be sane, surely? :(
				String workflowID = workflowJob.getWorkflow().getComponentInfo().getIdentificationInfo()
						.getIdentifiers().get(0).getValue();

				// make sure we have the workflow we want to run and get it's
				// details
				final String testWorkflowId = ensureHasWorkflow(client, workflowID);

				System.out.println("Workflow ID: " + testWorkflowId);

				final WorkflowDetails workflowDetails = client.showWorkflow(testWorkflowId);

				/**
				 * download the corpus from the OMTD-STORE using the REST
				 * client. This should get us a folder but not sure the format
				 * of the contents has been fixed yet
				 **/

				final File outputDir = new File("output");
				HistoriesClient historiesClient = instance.getHistoriesClient();

				String corpusId = workflowJob.getCorpusId();

				final String historyId = createHistory(instance, "OpenMinTeD Registry Integration: " + (new Date()));
				final List<String> ids = new ArrayList<String>();

				if (!corpusId.startsWith("file:")) {
					File corpusZip;
					try {
						corpusZip = File.createTempFile("corpus", ".zip");
						StoreResponse storeResponse = storeClient.downloadArchive(corpusId,
								corpusZip.getAbsolutePath());
					} catch (IOException e) {
						log.error("Unable to retrieve specified corpus with ID " + corpusId, e);
						status.put(workflowExecutionId, new ExecutionStatus(Status.FAILED));
						return;
					}

					// create a new history for this run and upload the input
					// files to it

					try (FileSystem zipFs = FileSystems.newFileSystem(corpusZip.toURI(), new HashMap<>());) {

						Path pathInZip = zipFs.getPath("/resources");

						Files.walkFileTree(pathInZip, new SimpleFileVisitor<Path>() {
							@Override
							public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs)
									throws IOException {

								// this will fail as the filePath is inside a
								// zip will probably have to copy to a temp file
								// before uploading, sigh :(
								OutputDataset dataset = upload(instance, historyId, filePath.toFile());

								// and the ID to the list
								ids.add(dataset.getId());

								return FileVisitResult.CONTINUE;
							}
						});

						waitForHistory(instance.getHistoriesClient(), historyId);

					} catch (IOException | InterruptedException e) {
						log.error("Unable to upload corpus to Galaxy history", e);
						status.put(workflowExecutionId, new ExecutionStatus(Status.FAILED));
						return;
					}
				} else {
					try {
						final File inputDir = toFile(new URL(corpusId));

						for (File f : inputDir.listFiles()) {
							OutputDataset dataset = upload(instance, historyId, f);
							ids.add(dataset.getId());
						}

						waitForHistory(instance.getHistoriesClient(), historyId);

					} catch (InterruptedException | MalformedURLException e) {
						log.error("Unable to upload corpus to Galaxy history", e);
						status.put(workflowExecutionId, new ExecutionStatus(Status.FAILED));
						return;
					}
				}

				status.put(workflowExecutionId, new ExecutionStatus(Status.RUNNING));

				/**
				 * run the workflow over the corups. can this be done in one
				 * shot or do we need to send each document in turn. Might
				 * depend on how the workflow is written?
				 **/

				for (String id : ids) {

					if (!shouldContinue(workflowExecutionId)) {
						log.debug("Workfloe execution " + workflowExecutionId + " stopped early");
					}

					// create a new workflow input in the correct history and
					// referencing the files we just uploaded as the inputs
					final WorkflowInputs inputs = new WorkflowInputs();
					inputs.setDestination(new ExistingHistory(historyId));
					inputs.setWorkflowId(testWorkflowId);
					inputs.setInput(getWorkflowInputId(workflowDetails, "Input Dataset"),
							new WorkflowInput(id, InputSourceType.HDA));

					// run the workflow and get a handle on the outputs produced
					final WorkflowOutputs output = client.runWorkflow(inputs);

					// make sure the workflow has finished and the history is in
					// the "ok" state before proceeding any further
					try {
						waitForHistory(historiesClient, output.getHistoryId());
					} catch (InterruptedException e) {
						// hmmmm that will mess things up
						log.error("Interrupted waiting for a valid Galaxy history", e);
						status.put(workflowExecutionId, new ExecutionStatus(Status.FAILED));
						return;
					}

					// we don't care about intermediary results so just retrieve
					// the final output from the workflow
					String outputId = output.getOutputIds().get(output.getOutputIds().size() - 1);

					// create a local file in which to store a copy of the
					// output
					File outputFile = new File(outputDir, outputId + ".txt");

					// download this output into the local file
					try {
						historiesClient.downloadDataset(output.getHistoryId(), outputId, outputFile);
					} catch (IOException e) {
						// if we can't download the file then we have a
						// problem....
						log.error("Unable to download result from Galaxy history", e);
						status.put(workflowExecutionId, new ExecutionStatus(Status.FAILED));
						return;
					}

					// as a bit of debugging print the file path and length
					// TODO this should add to a corpus going into the store
					System.out.println(outputFile.getAbsolutePath() + ": " + outputFile.length());
				}

				status.put(workflowExecutionId, new ExecutionStatus(Status.FINISHED));
			}
		};

		Thread t = new Thread(runner);
		t.start();

		/**
		 * return an ID that can be used to lookup the workflow later so you can
		 * get the status. this probably means spawning a thread for the actual
		 * execution so we can monitor the status of the Galaxy job as it runs
		 **/
		return workflowExecutionId.toString();
	}

	private boolean shouldContinue(String workflowExecutionId) {
		Status executionStatus = status.get(workflowExecutionId).getStatus();

		if (executionStatus == null)
			return true;

		while (executionStatus.equals(Status.PAUSED)) {
			try {
				Thread.sleep(200L);
				executionStatus = status.get(workflowExecutionId).getStatus();
			} catch (InterruptedException e) {
				log.error("something went wrong while wating for a paused workflow to be resumed");
				return false;
			}
		}

		return executionStatus.equals(Status.PENDING) || executionStatus.equals(Status.RUNNING);
	}

	@Override
	public void cancel(String workflowExecutionId) throws WorkflowException {
		if (!status.containsKey(workflowExecutionId)) return;
		
		status.put(workflowExecutionId, new ExecutionStatus(Status.CANCELED));
	}

	@Override
	public void pause(String workflowExecutionId) throws WorkflowException {
		if (!status.containsKey(workflowExecutionId)) return;
		
		status.put(workflowExecutionId, new ExecutionStatus(Status.PAUSED));
	}

	@Override
	public void resume(String workflowExecutionId) throws WorkflowException {
		if (!status.containsKey(workflowExecutionId)) return;
		
		if (!status.get(workflowExecutionId).getStatus().equals(Status.PAUSED)) return;
		
		status.put(workflowExecutionId, new ExecutionStatus(Status.RUNNING));
	}

	@Override
	public ExecutionStatus getExecutionStatus(String workflowExecutionId) throws WorkflowException {
		if (!status.containsKey(workflowExecutionId))
			throw new WorkflowException("Unknown Workflow Execution ID");

		return status.get(workflowExecutionId);
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
			throws WorkflowException {

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
		try {
			waitForHistory(instance.getHistoriesClient(), historyId);
		} catch (InterruptedException e) {
			// we were interrupted waiting for the history. turn this into an
			// exception we can throw
			throw new WorkflowException("Interrupted waiting for valid Galaxy history", e);
		}

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
				System.out.println("found workflow already in galaxy");

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
	
	private static File toFile(URL url) {
		try {
			return new File(url.toURI());
		} catch (URISyntaxException e) {
			return new File(url.getPath());
		}
	}
}
