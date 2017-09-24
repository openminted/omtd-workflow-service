package eu.openminted.workflow.service;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.stereotype.Component;

import com.github.jmchilton.blend4j.galaxy.GalaxyInstance;
import com.github.jmchilton.blend4j.galaxy.GalaxyInstanceFactory;
import com.github.jmchilton.blend4j.galaxy.ToolsClient;
import com.github.jmchilton.blend4j.galaxy.beans.Dataset;
import com.github.jmchilton.blend4j.galaxy.beans.History;
import com.github.jmchilton.blend4j.galaxy.beans.HistoryDetails;
import com.github.jmchilton.blend4j.galaxy.beans.JobDetails;
import com.github.jmchilton.blend4j.galaxy.beans.OutputDataset;
import com.github.jmchilton.blend4j.galaxy.beans.ToolExecution;
import com.github.jmchilton.blend4j.galaxy.beans.ToolInputs;
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

import eu.openminted.messageservice.connector.MessageServicePublisher;
import eu.openminted.messageservice.connector.MessageServiceSubscriber;
import eu.openminted.messageservice.connector.TopicsRegistry;
import eu.openminted.messageservice.messages.WorkflowExecutionStatusMessage;
import eu.openminted.store.common.StoreResponse;
import eu.openminted.store.restclient.StoreRESTClient;
import eu.openminted.workflow.api.ExecutionStatus;
import eu.openminted.workflow.api.ExecutionStatus.Status;
import eu.openminted.workflow.api.WorkflowException;
import eu.openminted.workflow.api.WorkflowJob;
import eu.openminted.workflow.api.WorkflowService;

@Component
public class WorkflowServiceImpl implements WorkflowService {

	private static final Logger log = LoggerFactory.getLogger(WorkflowServiceImpl.class);

	public final static String UNSET = "<UNSET>";
	
	private GalaxyInstance galaxy = null;

	// TODO how does this ever shrink?
	private static Map<String, ExecutionStatus> statusMonitor = new HashMap<String, ExecutionStatus>();

	// @Autowired
	MessageServicePublisher messageServicePublisher;

	// @Autowired
	MessageServiceSubscriber messageServiceSubscriber;

	// these should probably both be set via injection
	@Value("${galaxy.url}")
	String galaxyInstanceUrl;

	@Value("${galaxy.apiKey}")
	String galaxyApiKey;

	@Value("${store.endpoint}")
	String storeEndpoint;

	@Value("${galaxy.user.email:" + UNSET + "}")
	String galaxyUserEmail;

	@Value("${galaxy.ftp.dir:" + UNSET + "}")
	String galaxyFTPdir;

	public WorkflowServiceImpl() {
		log.info("Implementation:" + WorkflowServiceImpl.class.getName());
	}

	public WorkflowServiceImpl(MessageServicePublisher messageServicePublisher,
			MessageServiceSubscriber messageServiceSubscriber) {
		log.info("Implementation:" + WorkflowServiceImpl.class.getName());

		this.messageServicePublisher = messageServicePublisher;
		this.messageServiceSubscriber = messageServiceSubscriber;

		// Not the appropriate topics (used for testing).
		this.messageServiceSubscriber.addTopic(TopicsRegistry.workflowsExecution);
		this.messageServiceSubscriber.addTopic(TopicsRegistry.workflowsExecutionCompleted);
	}

	@Override
	public String execute(WorkflowJob workflowJob) throws WorkflowException {
		log.info("execute started");

		final String workflowExecutionId = UUID.randomUUID().toString();
		updateStatus(new ExecutionStatus(Status.PENDING), workflowExecutionId, TopicsRegistry.workflowsExecution);

		log.info("Starting workflow execution " + workflowExecutionId + " using Galaxy instance at "
				+ galaxyInstanceUrl);

		Runnable runner = new Runnable() {

			public void run() {
				if (!shouldContinue(workflowExecutionId))
					return;

				updateStatus(new ExecutionStatus(Status.RUNNING), workflowExecutionId,
						TopicsRegistry.workflowsExecution);

				StoreRESTClient storeClient = new StoreRESTClient(storeEndpoint);

				/**
				 * get workflow from job, it's a Component instance so will then
				 * need to get it out of there. question is what does that
				 * object contain and where is the workflow file actually
				 * stored?
				 **/

				String workflowName = workflowJob.getWorkflow().getMetadataHeaderInfo().getMetadataRecordIdentifier()
						.getValue();

				// make sure we have the workflow we want to run and get it's
				// details
				WorkflowDetails workflow = getWorkflowDetails(workflowName);

				if (workflow == null) {
					log.info("Unable to locate workflow: " + workflowName);

					updateStatus(new ExecutionStatus(new WorkflowException("Unable to locate named workflow")),
							workflowExecutionId, TopicsRegistry.workflowsExecution);
					return;
				}

				log.info("Workflow ID: " + workflow.getId());

				/**
				 * download the corpus from the OMTD-STORE using the REST
				 * client. This should get us a folder but not sure the format
				 * of the contents has been fixed yet
				 **/

				String corpusId = workflowJob.getCorpusId();

				if (!shouldContinue(workflowExecutionId))
					return;
				
				final History history = getGalaxy().getHistoriesClient()
						.create(new History("OpenMinTeD - " + (new Date())));

				File corpusZip;
				try {
					corpusZip = File.createTempFile("corpus", ".zip");
					String corpusZipFileName = corpusZip.getAbsolutePath();
					corpusZip.delete();
					log.info("download:" + corpusId + " to " + corpusZipFileName + " from " + storeEndpoint);
					StoreResponse storeResponse = storeClient.downloadArchive(corpusId, corpusZipFileName);

					if (!storeResponse.getResponse().startsWith("true")) {
						throw new IOException("Problem on downloading from STORE.");
					}
					log.info("corpusZipFileName:" + corpusZipFileName);
					log.info("corpusId:" + corpusId);

				} catch (IOException e) {
					log.info("Unable to retrieve specified corpus with ID " + corpusId, e);

					updateStatus(new ExecutionStatus(e), workflowExecutionId, TopicsRegistry.workflowsExecution);
					return;
				}
				
				if (!shouldContinue(workflowExecutionId))
					return;

				CollectionDescription collectionDescription = new CollectionDescription();
				collectionDescription.setCollectionType("list");
				collectionDescription.setName(history.getId() + "collection");

				try (FileSystem zipFs = FileSystems.newFileSystem(new URI("jar:" + corpusZip.getAbsoluteFile().toURI()),
						new HashMap<>());) {

					Path pathInZip = zipFs.getPath("/" + corpusId, "fulltext");

					Files.walkFileTree(pathInZip, new SimpleFileVisitor<Path>() {
						@Override
						public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs) throws IOException {

							// TODO check how filenames get used if there is a
							// folder inside fulltext as we don't want name
							// clashes if we end up flattening the dir structure

							OutputDataset dataset = null;
							Path path = null;

							if (galaxyFTPdir != null && !galaxyFTPdir.equals(UNSET)) {
								log.info("galaxyFTPdir:" + galaxyFTPdir);
								// it looks as if FTP support in Galaxy has been
								// enabled so we'll use that rather than doing
								// an HTTP upload which can be very slow

								// firstly we create a path to store this
								// specific file inside a folder named for the
								// Galaxy user's email address, and the workflow
								// execution ID
								path = Paths.get(galaxyFTPdir, galaxyUserEmail, workflowExecutionId,
										filePath.getFileName().toString());

								path.toFile().getParentFile().mkdirs();

								log.info("Copying file into FTP folder: " + path.toFile().getAbsolutePath().toString());

								// then we copy the file out of the zip
								path = Files.copy(filePath, path, StandardCopyOption.REPLACE_EXISTING);

								// once the file copy has completed Galaxy
								// should be able to see the file (the FTP
								// folder should be a shared filesystem between
								// this service and Galaxy) so we can just
								// attach the file to the history
								dataset = attachFTPUploadToHistory(history,
										workflowExecutionId + "/" + filePath.getFileName().toString());

							} else {
								// the Galaxy FTP folder isn't configured so
								// we'll need to upload the files directly into
								// the history

								// create a tmp file to hold the file from the
								// zip while we upload it to Galaxy

								try {
									log.info("Creating temp file:" + filePath.getFileName().toString());
									path = Files.createTempFile(null, filePath.getFileName().toString());
									path = Files.copy(filePath, path, StandardCopyOption.REPLACE_EXISTING);
									log.info("Uploading..." + path.toFile().getAbsolutePath());
									dataset = uploadFileToHistory(history.getId(), path.toFile());

								} finally {
									if (path != null)
										Files.delete(path);
								}
							}

							HistoryDatasetElement element = new HistoryDatasetElement();
							element.setId(dataset.getId());
							element.setName(path.toFile().getName());
							collectionDescription.addDatasetElement(element);

							return FileVisitResult.CONTINUE;

						}
					});

				} catch (IOException | URISyntaxException e) {
					log.error("Unable to upload corpus to Galaxy history", e);

					updateStatus(new ExecutionStatus(e), workflowExecutionId, TopicsRegistry.workflowsExecution);
					return;
				}
				
				if (!shouldContinue(workflowExecutionId))
					return;

				Path outputDir = null;

				CollectionResponse inputCollection = getGalaxy().getHistoriesClient()
						.createDatasetCollection(history.getId(), collectionDescription);

				try {
					outputDir = Files.createTempDirectory("omtd-workflow-output");
				} catch (IOException e) {
					log.error("Unable to create annotations output dir", e);

					updateStatus(new ExecutionStatus(e), workflowExecutionId, TopicsRegistry.workflowsExecution);
					return;
				}

				try {

					String workflowInputId = getWorkflowInputId(workflow, "Input Dataset Collection");

					log.info("Configuring input");
					WorkflowInvocationInputs workflowInputs = new WorkflowInvocationInputs();
					workflowInputs.setDestination(new WorkflowInputs.ExistingHistory(history.getId()));
					workflowInputs.setWorkflowId(workflow.getId());
					workflowInputs.setInput(workflowInputId, new WorkflowInputs.WorkflowInput(inputCollection.getId(),
							WorkflowInputs.InputSourceType.HDCA));

					setParameters(workflow, workflowInputs);
					printDetails(workflow, workflowInputs);

					if (!shouldContinue(workflowExecutionId))
						return;
					
					log.info("Run workflow");

					
					
					WorkflowInvocation workflowInvocation = getGalaxy().getWorkflowsClient()
							.invokeWorkflow(workflowInputs);

					String invocationID = workflowInvocation.getId();
					log.info("invocationID for " + workflow.getId() + " " + invocationID);

					log.info("count tools");
					int count = workflow.getSteps().size();
					log.info("tools counted:" + count);

					log.info("waitJobs");
					Map<String, WorkflowInvocationStepOutput> outputs = waitForInvocation(workflow.getId(),
							invocationID, count);

					if (!shouldContinue(workflowExecutionId))
						return;
					
					if (outputs == null) {
						// we failed to get the outputs
						log.debug("error", "there were no outputs from the invocation");
						updateStatus(new ExecutionStatus(ExecutionStatus.Status.FAILED), workflowExecutionId,
								TopicsRegistry.workflowsExecution);
						return;
					}

					log.info("waitHistory:");
					// TODO do we still need this check now we check the
					// invocation ouput?
					waitForHistory(history.getId());
					
					if (!shouldContinue(workflowExecutionId))
						return;
					
					// Jobs for this history have been completed
					// Also history is OK.
					// So, start downloading
					log.info("Starting download");
					downloadWorkflowOutputs(history.getId(), outputs, outputDir.toFile());
					log.info("Downloaded");
				} catch (Exception e) {
					e.printStackTrace();
					log.debug("error", e);
					updateStatus(new ExecutionStatus(e), workflowExecutionId, TopicsRegistry.workflowsExecution);
					return;
				}

				if (!shouldContinue(workflowExecutionId))
					return;
				
				try {
					String archiveID = uploadArchive(storeClient, outputDir);

					updateStatus(new ExecutionStatus(archiveID), workflowExecutionId,
							TopicsRegistry.workflowsExecutionCompleted);

				} catch (IOException e) {
					log.info("unable to store workflow results", e);

					updateStatus(new ExecutionStatus(e), workflowExecutionId, TopicsRegistry.workflowsExecution);
					return;
				}

				// TODO if we get to here then everything worked correctly, yay!
				// But what now? What cleanup should we do? Do we delete the
				// Galaxy history, the files uploaded via FTP, what about the
				// execution status map
			}
		};

		Thread t = new Thread(runner);
		t.start();

		// return an ID that can be used to track the progress of the invocation
		return workflowExecutionId.toString();
	}

	private void updateStatus(ExecutionStatus executionStatus, String workflowExecutionId, String topic) {
		try {
			Status status = executionStatus.getStatus();
			statusMonitor.put(workflowExecutionId, executionStatus);

			log.info("updateStatus:" + topic + "-->" + status);

			if (messageServicePublisher == null) {
				log.info("message service publisher not configured, message will be lost");
				return;
			}

			WorkflowExecutionStatusMessage msg = new WorkflowExecutionStatusMessage();
			msg.setWorkflowExecutionID(workflowExecutionId);
			msg.setWorkflowStatus(status.toString());

			if (Status.FINISHED.equals(status)) {
				msg.setResultingCorpusID(executionStatus.getCorpusID());
			}

			messageServicePublisher.publishMessage(topic, msg);
			log.info("updateStatus:" + topic + "-->" + status + " DONE");
		} catch (Exception e) {
			e.printStackTrace();
			log.debug("error", e);
		}
	}

	protected boolean shouldContinue(String workflowExecutionId) {
		Status executionStatus = statusMonitor.get(workflowExecutionId).getStatus();

		if (executionStatus == null)
			return true;

		while (executionStatus.equals(Status.PAUSED)) {
			try {
				Thread.sleep(200L);
				executionStatus = statusMonitor.get(workflowExecutionId).getStatus();
			} catch (InterruptedException e) {
				log.error("something went wrong while wating for a paused workflow to be resumed");
				return false;
			}
		}

		return executionStatus.equals(Status.PENDING) || executionStatus.equals(Status.RUNNING);
	}

	@Override
	public void cancel(String workflowExecutionId) throws WorkflowException {
		if (!statusMonitor.containsKey(workflowExecutionId))
			return;

		// you can't cancel if the workflow has finished or failed
		if (statusMonitor.get(workflowExecutionId).getStatus().equals(Status.FINISHED)
				|| statusMonitor.get(workflowExecutionId).getStatus().equals(Status.FAILED))
			return;

		updateStatus(new ExecutionStatus(Status.CANCELED), workflowExecutionId, TopicsRegistry.workflowsExecution);
	}

	@Override
	public void pause(String workflowExecutionId) throws WorkflowException {
		if (!statusMonitor.containsKey(workflowExecutionId))
			return;

		// you can't pause unless the workflow is pending or running
		if (!statusMonitor.get(workflowExecutionId).getStatus().equals(Status.PENDING)
				&& !statusMonitor.get(workflowExecutionId).getStatus().equals(Status.RUNNING))
			return;

		updateStatus(new ExecutionStatus(Status.PAUSED), workflowExecutionId, TopicsRegistry.workflowsExecution);
	}

	@Override
	public void resume(String workflowExecutionId) throws WorkflowException {
		if (!statusMonitor.containsKey(workflowExecutionId))
			return;

		// you can't resume a workflow that isn't paused
		if (!statusMonitor.get(workflowExecutionId).getStatus().equals(Status.PAUSED))
			return;

		updateStatus(new ExecutionStatus(Status.RUNNING), workflowExecutionId, TopicsRegistry.workflowsExecution);
	}

	@Override
	public ExecutionStatus getExecutionStatus(String workflowExecutionId) throws WorkflowException {
		if (!statusMonitor.containsKey(workflowExecutionId))
			throw new WorkflowException("Unknown Workflow Execution ID");

		return statusMonitor.get(workflowExecutionId);
	}

	@Override
	public void delete(eu.openminted.registry.domain.Component workflow) throws WorkflowException {
		String workflowName = workflow.getMetadataHeaderInfo().getMetadataRecordIdentifier().getValue();

		// make sure we have the workflow we want to delete
		WorkflowDetails workflowDetails = getWorkflowDetails(workflowName);
		log.info("found workflow ID" + workflowName + "/" + workflow);
		if (workflowDetails == null)
			return;

		getGalaxy().getWorkflowsClient().deleteWorkflowRequest(workflowDetails.getId());
	}

	private static String uploadArchive(StoreRESTClient storeClient, Path archiveData) throws IOException {
		String archiveID = storeClient.createArchive().getResponse();
		String annotationsFolderId = storeClient.createSubArchive(archiveID, "annotations").getResponse();

		Files.walk(archiveData).filter(path -> !Files.isDirectory(path)).forEach(path -> {
			storeClient.storeFile(path.toFile(), annotationsFolderId, path.getFileName().toString());
		});

		storeClient.finalizeArchive(archiveID);

		return archiveID;
	}

	// ---
	// ---
	public static void main(String[] args) throws Exception {
		SpringApplication.run(WorkflowServiceImpl.class, args);
	}

	private GalaxyInstance getGalaxy() {
		if (galaxy != null)
			return galaxy;

		galaxy = GalaxyInstanceFactory.get(galaxyInstanceUrl, galaxyApiKey);

		return galaxy;
	}

	private WorkflowDetails getWorkflowDetails(final String workflowName) {
		String workflowId = null;
		for (Workflow workflow : getGalaxy().getWorkflowsClient().getWorkflows()) {
			log.info(workflow.getName() + " " + workflowName);
			if (workflow.getName().startsWith(workflowName)) {
				workflowId = workflow.getId();
				break;
			}
		}

		if (workflowId == null)
			return null;
		return getGalaxy().getWorkflowsClient().showWorkflow(workflowId);
	}

	private OutputDataset attachFTPUploadToHistory(History history, String path) {

		ToolsClient toolsClient = getGalaxy().getToolsClient();

		// we need a map to hold the options which will become a JSON map
		Map<String, String> ftpUploadSettings = new HashMap<String, String>();

		// specify that we are uploading a dataset
		ftpUploadSettings.put("files_0|type", "upload_dataset");

		// relative path within the FTP folder for the current user to the file
		// we want to add to the history
		ftpUploadSettings.put("files_0|ftp_files", path);

		// this must reference upload1 as that seems to be the internal name for
		// the tool in Galaxy
		ToolInputs ftpUpload = new ToolInputs("upload1", ftpUploadSettings);
		ToolExecution toolReturn = toolsClient.create(history, ftpUpload);

		return toolReturn.getOutputs().get(0);
	}

	private OutputDataset uploadFileToHistory(final String historyId, final File file) {
		final ToolsClient.FileUploadRequest request = new ToolsClient.FileUploadRequest(historyId, file);
		final ToolExecution execution = getGalaxy().getToolsClient().upload(request);
		return execution.getOutputs().get(0);
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

	private void setParameters(WorkflowDetails workflowDetails, WorkflowInputs inputs) {

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

	private void printDetails(WorkflowDetails workflowDetails, WorkflowInputs inputs) {

		for (final Map.Entry<String, WorkflowStepDefinition> entry : workflowDetails.getSteps().entrySet()) {
			final String stepId = entry.getKey();
			final WorkflowStepDefinition stepDef = entry.getValue();

			log.info(stepId + " " + stepDef.getType());

			for (final Map.Entry<String, WorkflowStepDefinition.WorkflowStepOutput> stepInput : stepDef.getInputSteps()
					.entrySet()) {
				log.info("Parameter " + stepInput.getKey() + "-" + stepInput.getValue() + "\n");
			}

		}
	}

	public void waitForHistory(final String historyId) throws InterruptedException {

		// a placeholder for the current details of the history
		HistoryDetails details = null;

		// wait until the history is in the ready state
		while (true) {
			details = getGalaxy().getHistoriesClient().showHistory(historyId);
			if (details.isReady()) {
				break;
			} else if (details.getState().equals("error")) {
				throw new RuntimeException("history is in the error state so it will never be usable");
			}

			// don't hammer the galaxy instance too heavily
			Thread.sleep(200L);
		}

		// if the history is in an ok state then we can return
		String state = null;
		while (true) {
			details = getGalaxy().getHistoriesClient().showHistory(historyId);
			state = details.getState();
			if (state.equals("ok")) {
				System.out.println(details.getStateIds());
				return;
			} else if (state.equals("error")) {
				throw new RuntimeException("history is in the error state so it will never be usable");
			}

			Thread.sleep(1000L);

		}
	}

	public Map<String, WorkflowInvocationStepOutput> waitForInvocation(String workflowId, String invocationId,
			int stepCount) throws InterruptedException {

		WorkflowInvocation invocation = null;

		// wait for all steps to have been added to the invocation
		while (true) {

			try {
				invocation = getGalaxy().getWorkflowsClient().showInvocation(workflowId, invocationId);

				if (invocation == null) {
					log.info("invocation is null..returning");
					// TODO should probably be an exception instead
					return null;
				}

				log.info(invocation.getWorkflowSteps().size() + " == " + stepCount);

				if (invocation.getWorkflowSteps().size() == stepCount) {
					break;
				}
			} catch (Exception e) {
				e.printStackTrace();
				log.info(e.getMessage());
				return null;
			}

			Thread.sleep(200);
		}

		while (true) {
			try {
				invocation = getGalaxy().getWorkflowsClient().showInvocation(workflowId, invocationId);

				if (invocation == null) {
					log.info("invocation is null..returning");
					return null;
				}

				log.info("workflow steps size:" + invocation.getWorkflowSteps().size());
				WorkflowInvocationStep step = invocation.getWorkflowSteps().get(stepCount - 1);

				if (step != null) {
					log.info("Step state:" + step.getState());

					if (step.getState().equals("error")) {
						return null;
					}

					String jobId = step.getJobId();

					JobDetails jobDetails = getGalaxy().getJobsClient().showJob(jobId);
					log.info("JOB DETAILS: " + jobDetails.getState() + "|" + jobDetails.getExitCode());

				}

				if (step != null && step.getState() != null && step.getState().equals("ok")) {

					while (true) {
						String jobId = step.getJobId();
						JobDetails jobDetails = getGalaxy().getJobsClient().showJob(jobId);
						log.info("JOB DETAILS: " + jobDetails.getState() + "|" + jobDetails.getExitCode());

						if (jobDetails.getState().equals("ok") && jobDetails.getExitCode() != null) {

							while (true) {
								step = getGalaxy().getWorkflowsClient().showInvocationStep(workflowId, invocationId,
										step.getId());
								System.out.println("Step outputs: " + step.getOutputs());
								if (step.getOutputs() != null && step.getOutputs().size() > 0)
									return step.getOutputs();
							}
						}

					}

				}
			} catch (Exception e) {
				e.printStackTrace();
				log.info(e.getMessage());
			}

			Thread.sleep(200);
		}
	}

	private void downloadWorkflowOutputs(String historyId, Map<String, WorkflowInvocationStepOutput> outputs,
			File dir) {

		// download this output into the local file
		try {

			for (Map.Entry<String, WorkflowInvocationStepOutput> output : outputs.entrySet()) {

				log.info(output.getKey() + "|" + output.getValue().getType() + "|" + output.getValue().getId());

				if (output.getValue().getType().equals(WorkflowInputs.InputSourceType.HDA)) {
					Dataset dataset = getGalaxy().getHistoriesClient().showDataset(historyId,
							output.getValue().getId());
					if (!dir.exists()) {
						dir.mkdirs();
					}
					File outputFile = new File(dir, dataset.getName());
					log.info("downloading " + dataset.getName() + " to " + outputFile.getAbsolutePath());
					getGalaxy().getHistoriesClient().downloadDataset(historyId, output.getValue().getId(), outputFile);
				}
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
