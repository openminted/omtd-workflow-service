package eu.openminted.workflow.service;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import com.github.jmchilton.blend4j.galaxy.GalaxyInstance;
import com.github.jmchilton.blend4j.galaxy.GalaxyInstanceFactory;
import com.github.jmchilton.blend4j.galaxy.ToolsClient;
import com.github.jmchilton.blend4j.galaxy.WorkflowsClient;
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

import eu.openminted.corpus.OMTDCorpus;
import eu.openminted.registry.core.service.ServiceException;
import eu.openminted.registry.domain.DataFormatType;
import eu.openminted.registry.domain.ResourceIdentifier;
import eu.openminted.registry.domain.ResourceIdentifierSchemeNameEnum;
import eu.openminted.store.common.StoreResponse;
import eu.openminted.store.restclient.StoreRESTClient;
import eu.openminted.workflow.api.ExecutionStatus;
import eu.openminted.workflow.api.ExecutionStatus.Status;
import eu.openminted.workflow.api.WorkflowException;
import eu.openminted.workflow.api.WorkflowExecutionStatusMessage;
import eu.openminted.workflow.api.WorkflowJob;
import eu.openminted.workflow.api.WorkflowService;
import eu.openminted.workflow.beans.JmsConfiguration;

@Component
public class WorkflowServiceImpl implements WorkflowService {

	private static final Logger log = LoggerFactory.getLogger(WorkflowServiceImpl.class);

	public final static String UNSET = "<UNSET>";
	
	private static final long WAIT_TIME = 2000;

	private GalaxyInstance galaxy = null;

	private static Map<String, WorkflowExecution> statusMonitor = new HashMap<String, WorkflowExecution>();

	private JmsTemplate jmsQueueTemplate;

	@Autowired
	private JmsConfiguration jmsConfiguration;

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

	@Value("${omtd.workflow.debug:true}")
	Boolean debug = Boolean.TRUE;

	@Autowired
	public WorkflowServiceImpl(JmsTemplate jmsQueueTemplate) {
		log.info("Implementation:" + WorkflowServiceImpl.class.getName());
		this.jmsQueueTemplate = jmsQueueTemplate;
	}

	@Override
	public String execute(WorkflowJob workflowJob) throws WorkflowException {
		log.info("execute started");

		final String workflowExecutionId = UUID.randomUUID().toString();

		statusMonitor.put(workflowExecutionId, new WorkflowExecution(workflowExecutionId));

		// TODO fix userID getting it from workflowJob (when connection to redis exist)
		updateStatus(
				new ExecutionStatus("0931731143127784@openminted.eu", workflowJob.getCorpusId(),
						workflowJob.getWorkflow().getMetadataHeaderInfo().getMetadataRecordIdentifier().getValue()),
				workflowExecutionId, null);

		log.info("Starting workflow execution " + workflowExecutionId + " using Galaxy instance at "
				+ galaxyInstanceUrl);
		
		Runnable runner = new Runnable() {

			@Override
			public void run() {
				if (!shouldContinue(workflowExecutionId))
					return;

				updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(), Status.RUNNING), workflowExecutionId, "collecting workflow data and corpora");

				StoreRESTClient storeClient = new StoreRESTClient(storeEndpoint);

				
				// * get workflow from job, it's a Component instance so will then
				// * need to get it out of there. question is what does that
				// * object contain and where is the workflow file actually
				// * stored?
				 

				String workflowName = resolveApplicationWorkflow(workflowJob.getWorkflow());
				
				try {
					workflowName = URLDecoder.decode(workflowName, "UTF-8");
				} catch (UnsupportedEncodingException e1) {
					//should never happen
				}
				
				// make sure we have the workflow we want to run and get it's
				// details
				WorkflowDetails workflow = getWorkflowDetails(workflowName);

				if (workflow == null) {
					log.info("Unable to locate workflow: " + workflowName);

					updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),new WorkflowException("Unable to locate named workflow")),
							workflowExecutionId,null);
					return;
				}

				log.info("Workflow ID: " + workflow.getId());

				statusMonitor.get(workflowExecutionId).setWorkflowId(workflow.getId());

				//
				// * download the corpus from the OMTD-STORE using the REST
				// * client. This should get us a folder but not sure the format
				// * of the contents has been fixed yet
				// *

				String corpusId = workflowJob.getCorpusId();
				
				statusMonitor.get(workflowExecutionId).setCorpusId(corpusId);

				if (!shouldContinue(workflowExecutionId))
					return;

				
				StoreResponse storeResponse = storeClient.archiveExists(corpusId);
				if (!Boolean.valueOf(storeResponse.getResponse())) {
					// corpus doesn't exist in store
					updateStatus(
							new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),
									new WorkflowException("corpus with ID '" + corpusId + "' does not exist")),
							workflowExecutionId,null);
					
					return;
				}
				
				// not sure if we should ignore zip files or not but I don't any
				// of the components will be supporting zip files so....
				List<String> files = storeClient.listFiles(corpusId, false, true, true);
				checkCorpusHasFilesToProcess(files, workflowJob.getSubArchive(),  workflowExecutionId, corpusId);
				
				if (!shouldContinue(workflowExecutionId))
					return;
				
				final History history = getGalaxy().getHistoriesClient()
						.create(new History(workflowName + " - " + corpusId + ": "+(new Date())));

				boolean workflowContainsImporter = workflowContainsOMTDImporter(workflow);

				WorkflowInvocationInputs workflowInputs = new WorkflowInvocationInputs();
				workflowInputs.setDestination(new WorkflowInputs.ExistingHistory(history.getId()));
				workflowInputs.setWorkflowId(workflow.getId());

				if (!workflowContainsImporter) {
					
					updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),Status.RUNNING), workflowExecutionId, "Copying corpus to workflow executor");

					File corpusZip;
					try {
						corpusZip = File.createTempFile("corpus", ".zip");
						String corpusZipFileName = corpusZip.getAbsolutePath();
						corpusZip.delete();
						log.info("download:" + corpusId + " to " + corpusZipFileName + " from " + storeEndpoint);
						storeResponse = storeClient.downloadArchive(corpusId, corpusZipFileName);

						if (!storeResponse.getResponse().startsWith("true")) {
							throw new IOException("Problem on downloading from STORE.");
						}
						log.info("corpusZipFileName:" + corpusZipFileName);
						log.info("corpusId:" + corpusId);

					} catch (IOException e) {
						log.info("Unable to retrieve specified corpus with ID " + corpusId, e);

						updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),e), workflowExecutionId, null);
						return;
					}

					if (!shouldContinue(workflowExecutionId))
						return;

					CollectionDescription collectionDescription = new CollectionDescription();
					collectionDescription.setCollectionType("list");
					collectionDescription.setName(history.getId() + "collection");

					try (FileSystem zipFs = FileSystems
							.newFileSystem(new URI("jar:" + corpusZip.getAbsoluteFile().toURI()), new HashMap<>());) {

						Path pathInZip = zipFs.getPath("/" + corpusId, workflowJob.getSubArchive());

						Files.walkFileTree(pathInZip, new SimpleFileVisitor<Path>() {
							@Override
							public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs)
									throws IOException {

								// TO-DO: check how filenames get used if there is
								// a folder inside subarchive folder (e.g. fulltext) as we don't want name
								// clashes if we end up flattening the dir
								// structure

								OutputDataset dataset = null;
								Path path = null;

								if (galaxyFTPdir != null && !galaxyFTPdir.equals(UNSET)) {
									log.info("galaxyFTPdir:" + galaxyFTPdir);
									// it looks as if FTP support in Galaxy has
									// been
									// enabled so we'll use that rather than
									// doing
									// an HTTP upload which can be very slow

									// firstly we create a path to store this
									// specific file inside a folder named for
									// the
									// Galaxy user's email address, and the
									// workflow
									// execution ID
									path = Paths.get(galaxyFTPdir, galaxyUserEmail, workflowExecutionId,
											filePath.getFileName().toString());

									path.toFile().getParentFile().mkdirs();

									log.info("Copying file into FTP folder: "
											+ path.toFile().getAbsolutePath().toString());

									// then we copy the file out of the zip
									path = Files.copy(filePath, path, StandardCopyOption.REPLACE_EXISTING);

									// once the file copy has completed Galaxy
									// should be able to see the file (the FTP
									// folder should be a shared filesystem
									// between
									// this service and Galaxy) so we can just
									// attach the file to the history
									dataset = attachFTPUploadToHistory(history,
											workflowExecutionId + "/" + filePath.getFileName().toString());

								} else {
									// the Galaxy FTP folder isn't configured so
									// we'll need to upload the files directly
									// into
									// the history

									// create a tmp file to hold the file from
									// the
									// zip while we upload it to Galaxy

									try {
										log.info("Creating temp file:" + filePath.getFileName().toString());
										path = Files.createTempFile(null, filePath.getFileName().toString());
										path = Files.copy(filePath, path, StandardCopyOption.REPLACE_EXISTING);
										log.info("Uploading..." + path.toFile().getAbsolutePath());
										dataset = uploadFileToHistory(history.getId(), path.toFile());

									} finally {
										if (path != null && !debug)
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

						updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),e), workflowExecutionId,null);
						return;
					}

					if (!shouldContinue(workflowExecutionId))
						return;

					CollectionResponse inputCollection = getGalaxy().getHistoriesClient()
							.createDatasetCollection(history.getId(), collectionDescription);

					String workflowInputId = getWorkflowInputId(workflow, "Input Dataset Collection");

					log.info("Configuring input");
					
					updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),Status.RUNNING), workflowExecutionId, "configuing workflow input");

					workflowInputs.setInput(workflowInputId, new WorkflowInputs.WorkflowInput(inputCollection.getId(),
							WorkflowInputs.InputSourceType.HDCA));
				}else{
					log.info("Configuring " + OMTDImporter.class.getName());
					
					log.info("Setting corpus ID on " + OMTDImporter.class.getName() + " " + corpusId);
					workflowInputs.setStepParameter("0", OMTDImporter.omtdStoreCorpusID, corpusId);
					log.info("Setting subarchive on " + OMTDImporter.class.getName() + " " + workflowJob.getSubArchive());
					workflowInputs.setStepParameter("0", OMTDImporter.omtdSubArchive, workflowJob.getSubArchive());
				}

				//setParameters(workflow, workflowInputs);
				printDetails(workflow, workflowInputs);

				if (!shouldContinue(workflowExecutionId))
					return;

				Path outputDir = null;

				try {
					log.info("Run workflow");

					WorkflowInvocation workflowInvocation = getGalaxy().getWorkflowsClient()
							.invokeWorkflow(workflowInputs);

					String invocationID = workflowInvocation.getId();

					statusMonitor.get(workflowExecutionId).setInvocationId(invocationID);

					log.info("invocationID for " + workflow.getId() + " " + invocationID);

					log.info("count tools");
					int count = workflow.getSteps().size();
					log.info("tools counted:" + count);

					log.info("waitJobs");
					Map<String, WorkflowInvocationStepOutput> outputs = waitForInvocation(workflowExecutionId, workflow.getId(),
							invocationID, count);

					if (!shouldContinue(workflowExecutionId))
						return;

					if (outputs == null || outputs.isEmpty()) {
						// we failed to get the outputs
						log.debug("error", "there were no outputs from the invocation");
						updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),new RuntimeException("there were no outputs from workflow, workflow probably failed")), workflowExecutionId, null);
						return;
					}

					log.info("waitHistory:");
					// TODO do we still need this check now we check the
					// invocation ouput?
					waitForHistory(history.getId());
					
					updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),Status.RUNNING), workflowExecutionId, "collecting workflow outputs");

					if (!shouldContinue(workflowExecutionId))
						return;

					try {
						outputDir = Files.createTempDirectory("omtd-workflow-output");
					} catch (IOException e) {
						log.error("Unable to create annotations output dir", e);

						updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),e), workflowExecutionId,null);
						return;
					}

					// Jobs for this history have been completed
					// Also history is OK.
					// So, start downloading
					log.info("Starting download");
					downloadWorkflowOutputs(history.getId(), outputs, outputDir.toFile());
					log.info("Downloaded");
				} catch (Exception e) {
					e.printStackTrace();
					log.debug("error", e);
					updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),e), workflowExecutionId, null);
					return;
				}

				if (!shouldContinue(workflowExecutionId))
					return;

				try {
					// Decide folder name for results.
					String folderNameForResults = getFolderNameForResults(workflowJob);
					// Upload archive with results to store.
					String archiveID = uploadArchive(storeClient, corpusId, folderNameForResults, outputDir);
					// Update status
					ExecutionStatus finishedStat = new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),archiveID);
					log.info("finished archiveID" + archiveID);
					updateStatus(finishedStat, workflowExecutionId, "workflow successfully completed");

				} catch (IOException e) {
					log.info("unable to store workflow results", e);

					updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),e), workflowExecutionId, null);
					return;
				}

				// TODO this should probably be in a try/finally so we clean up
				// even if we error out part way through unless we are in debug
				// mode when we will risk running out of disk space etc.
				if (!debug) {
					// remove ourselves from the status monitor as the corpus ID
					// will have been passed back to the registry at this point
					// via the message queue
					statusMonitor.remove(workflowExecutionId);

					// we no longer need the Galaxy history so we can delete
					// that as well to free up space on the Galaxy server and
					// so we don't run out of space on the NFS
					getGalaxy().getHistoriesClient().deleteHistory(history.getId(), true);

					if (galaxyFTPdir != null && !galaxyFTPdir.equals(UNSET)) {
						// if we used the FTP upload option then delete the
						// folder we created for this invocation
						try {
							Path path = Paths.get(galaxyFTPdir, galaxyUserEmail, workflowExecutionId);
							FileUtils.deleteDirectory(path.toFile());
						} catch (IOException e) {
							// if we can't delete the files then so what?
							log.warn("unable to delete all files from FTP folder", e);
						}
					}
				}
			}
		};

		Thread t = new Thread(runner);
		t.start();

		// return an ID that can be used to track the progress of the invocation
		return workflowExecutionId.toString();
	}

	private String getFolderNameForResults(WorkflowJob workflowJob){
		
		String folder =  OMTDCorpus.subArchiveAnnotations;
		
		try{
			folder = workflowJob.getWorkflow().getComponentInfo().getOutputResourceInfo()
					.getDataFormats().get(0).getDataFormat()
					.equals(DataFormatType.HTTP___W3ID_ORG_META_SHARE_OMTD_SHARE_XMI) ? OMTDCorpus.subArchiveAnnotations
							: OMTDCorpus.subArchiveOutput;
		}catch(Exception e){
			log.info("Problem on selecting folder name.");
		}
		
		return folder;
	}
	
	
	private void updateStatus(ExecutionStatus executionStatus, String workflowExecutionId, String statusDescription) {
		try {
			
			// Updated local status monitor.
			WorkflowExecution executionDetails = statusMonitor.get(workflowExecutionId); 
			executionDetails.setExecutionStatus(executionStatus);
			
			WorkflowExecution we = statusMonitor.get(workflowExecutionId);
			log.info("local status monitor:" + we.getCorpusId() + " " + we.getExecutionId());
			
			
			String topic = jmsConfiguration.getWorkflowsExecution();
			
			String invocationID = executionDetails.getInvocationId();
			
			// Build the required msg and send it to JMS.
			Status status = executionStatus.getStatus();
			log.info("updateStatus:" + topic + "-->" + status);
			WorkflowExecutionStatusMessage msg = new WorkflowExecutionStatusMessage();
			msg.setWorkflowExecutionID(workflowExecutionId);
			msg.setWorkflowStatus(status.toString());
			msg.setWorkflowStatusDescription(statusDescription);

			msg.setCorpusID(executionStatus.getCorpusID());
			msg.setUserID(executionStatus.getUserID());
			msg.setWorkflowID(executionStatus.getWorkflowId()); 
			
			if (Status.FINISHED.equals(status)) {
				msg.setResultingCorpusID(executionStatus.getCorpusID());
			}
			else if (Status.FAILED.equals(status)) {
				msg.setError(executionStatus.getFailureCause().getMessage());
			}
			
			if (invocationID != null) {
				msg.setWorkflowInvocationID(invocationID);
				
				try {
					
					WorkflowsClient workflowsClient = getGalaxy().getWorkflowsClient();
					
					WorkflowInvocation details = workflowsClient.showInvocation(executionDetails.workflowId, invocationID);
					
					List<String> jobIDs = new ArrayList<String>();
					for (WorkflowInvocationStep step : details.getWorkflowSteps()) {
						jobIDs.add(step.getJobId());
					}
					
					msg.setWorkflowJobIDs(String.join("|", jobIDs));
				}
				catch (Exception e) {
					log.warn("unable to get invocation job IDs", e);
				}
			}

			log.info("Sending to registry msg ::" + msg.toString());
			jmsQueueTemplate.convertAndSend(topic,msg);
			//messageServicePublisher.publishMessage(topic, msg);
			log.info("updateStatus:" + topic + "-->" + status + " DONE");
		} catch (Exception e) {
			e.printStackTrace();
			log.debug("error", e);
		}
	}

	protected boolean shouldContinue(String workflowExecutionId) {
		Status executionStatus = statusMonitor.get(workflowExecutionId).getExecutionStatus().getStatus();

		if (executionStatus == null)
			return true;

		while (executionStatus.equals(Status.PAUSED)) {
			try {
				Thread.sleep(WAIT_TIME);
				executionStatus = statusMonitor.get(workflowExecutionId).getExecutionStatus().getStatus();
			} catch (InterruptedException e) {
				log.error("something went wrong while waiting for a paused workflow to be resumed");
				return false;
			}
		}

		return executionStatus.equals(Status.PENDING) || executionStatus.equals(Status.RUNNING);
	}

	@Override
	public void cancel(String workflowExecutionId) throws WorkflowException {
		if (!statusMonitor.containsKey(workflowExecutionId))
			return;

		WorkflowExecution workflowExecution = statusMonitor.get(workflowExecutionId);

		Status status = workflowExecution.getExecutionStatus().getStatus();

		// you can't cancel if the workflow has finished or failed
		if (status.equals(Status.FINISHED) || status.equals(Status.FAILED))
			return;

		if (workflowExecution.getInvocationId() != null) {
			// if we've actually invoked the workflow in Galaxy then lets try
			// and cancel that
			getGalaxy().getWorkflowsClient().cancelWorkflowInvocation(workflowExecution.getWorkflowId(),
					workflowExecution.getInvocationId());
		}

		updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),Status.CANCELED), workflowExecutionId, null);
	}

	@Override
	public void pause(String workflowExecutionId) throws WorkflowException {
		if (!statusMonitor.containsKey(workflowExecutionId))
			return;

		Status status = statusMonitor.get(workflowExecutionId).getExecutionStatus().getStatus();

		// you can't pause unless the workflow is pending or running
		if (!status.equals(Status.PENDING) && !status.equals(Status.RUNNING))
			return;

		updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),Status.PAUSED), workflowExecutionId, null);
	}

	@Override
	public void resume(String workflowExecutionId) throws WorkflowException {
		if (!statusMonitor.containsKey(workflowExecutionId))
			return;

		Status status = statusMonitor.get(workflowExecutionId).getExecutionStatus().getStatus();

		// you can't resume a workflow that isn't paused
		if (!status.equals(Status.PAUSED))
			return;

		updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),Status.RUNNING), workflowExecutionId, null);
	}

	@Override
	public ExecutionStatus getExecutionStatus(String workflowExecutionId) throws WorkflowException {
		if (!statusMonitor.containsKey(workflowExecutionId))
			throw new WorkflowException("Unknown Workflow Execution ID");

		return statusMonitor.get(workflowExecutionId).getExecutionStatus();
	}

	@Override
	public void delete(eu.openminted.registry.domain.Component workflow) throws WorkflowException {
		String workflowName = workflow.getMetadataHeaderInfo().getMetadataRecordIdentifier().getValue();

		// make sure we have the workflow we want to delete
		WorkflowDetails workflowDetails = getWorkflowDetails(workflowName);
		log.info("found workflow ID" + workflowName + "/" + workflow);
		if (workflowDetails == null)
			return;

		getGalaxy().getWorkflowsClient().deleteWorkflow(workflowDetails.getId());
	}

	private static String uploadArchive(StoreRESTClient storeClient, String corpusId, String folderWithResults, Path archiveData) throws IOException {
		log.info("uploadArchive");
		log.info("initial corpus:" + corpusId + " folder:" + folderWithResults);
		
		String clonedArchiveID = storeClient.cloneArchive(corpusId).getResponse();
		log.info("clone corpus:" + clonedArchiveID);
		String annotationsFolderId = storeClient.createSubArchive(clonedArchiveID, folderWithResults).getResponse();
		log.info("annotationsFolderId:" + annotationsFolderId);
		
		Files.walk(archiveData).filter(path -> !Files.isDirectory(path)).forEach(path -> {
			storeClient.storeFile(path.toFile(), annotationsFolderId, path.getFileName().toString());
		});

		storeClient.finalizeArchive(clonedArchiveID);
		log.info("finalizeArchive:" + clonedArchiveID);
		
		return clonedArchiveID;
	}

	// ---
	// ---
	public static void main(String[] args) throws Exception {
		SpringApplication.run(WorkflowServiceImpl.class, args);
	}

	private GalaxyInstance getGalaxy() {
		if (galaxy != null)
			return galaxy;

		log.info("galaxyInstanceUrl:" + galaxyInstanceUrl);
		log.info("galaxyApiKey:" + galaxyApiKey);
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

	private boolean workflowContainsOMTDImporter(WorkflowDetails workflow){
		boolean  containsOMTDImporter = "omtdImporter".equals(workflow.getSteps().get("0").getToolID());
		log.info("Workflow starts with OMTD Importer: " + containsOMTDImporter);
		return containsOMTDImporter;
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
			Thread.sleep(WAIT_TIME);
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

			Thread.sleep(WAIT_TIME);

		}
	}

	public Map<String, WorkflowInvocationStepOutput> waitForInvocation(String workflowExecutionId, String workflowId, String invocationId,
			int stepCount) throws InterruptedException {

		WorkflowInvocation invocation = null;

		// wait for all steps to have been added to the invocation
		while (true) {
			invocation = getGalaxy().getWorkflowsClient().showInvocation(workflowId, invocationId);

			if (invocation == null) {
				log.info("invocation is null..returning");
				throw new RuntimeException("Invocation can't be found");
			}

			log.info(invocation.getWorkflowSteps().size() + " == " + stepCount);

			if (invocation.getWorkflowSteps().size() == stepCount) {
				break;
			}
			
			Thread.sleep(WAIT_TIME);
		}
		
		updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),Status.RUNNING), workflowExecutionId, "all workflow steps have been initialised");

		while (true) {
			
			invocation = getGalaxy().getWorkflowsClient().showInvocation(workflowId, invocationId);

			if (invocation == null) {
				log.info("invocation is null..returning");
				throw new RuntimeException("Invocation can't be found");
			}

			log.info("workflow steps size:" + invocation.getWorkflowSteps().size());
			
			for (WorkflowInvocationStep step : invocation.getWorkflowSteps()) {
				//double check none of the steps are in the error state
				if (step != null && "error".equals(step.getState())) {
					throw new RuntimeException("a workflow state is in error: "+workflowId+"/"+invocationId+"/"+step.getId()+"/"+step.getJobId());
				}
			}
			
			WorkflowInvocationStep step = invocation.getWorkflowSteps().get(stepCount - 1);
			
			if (step != null) {
				log.info("Step state:" + step.getState());

				if ("error".equals(step.getState())) {
					throw new RuntimeException("final workflow state is in error: "+workflowId+"/"+invocationId+"/"+step.getId()+"/"+step.getJobId());
				}

				String jobId = step.getJobId();

				JobDetails jobDetails = getGalaxy().getJobsClient().showJob(jobId);
				log.info("JOB DETAILS: " + jobDetails.getState() + "|" + jobDetails.getExitCode());

			}

			if (step != null && step.getState() != null && step.getState().equals("ok")) {
				
				updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),Status.RUNNING), workflowExecutionId, "final workflow step is running");

				while (true) {
					String jobId = step.getJobId();
					JobDetails jobDetails = getGalaxy().getJobsClient().showJob(jobId);
					log.info("JOB DETAILS: " + jobDetails.getState() + "|" + jobDetails.getExitCode());

					if (jobDetails.getState().equals("ok") && jobDetails.getExitCode() != null) {
						
						updateStatus(new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),Status.RUNNING), workflowExecutionId, "final workflow step has completed, collecting output");

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
			
			Thread.sleep(WAIT_TIME);
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

	public void checkCorpusHasFilesToProcess(List<String> files, String subArchive, String workflowExecutionId, String corpusId){
		log.info("checkCorpusHasFilesToProcess");
		
		//filter the files to just keep paths inside the subArchive folder
		List<String> toBeProcessed = files.stream().filter(f -> f.indexOf("/" + subArchive + "/") > -1).collect(Collectors.toList());
		
		if (toBeProcessed == null || toBeProcessed.isEmpty()) {
			log.info("checkCorpusHasFilesToProcess->Problem with to be processed folder" );
			updateStatus(
					new ExecutionStatus(statusMonitor.get(workflowExecutionId).getExecutionStatus(),
							new WorkflowException("corpus with ID '" + corpusId + "' is empty")),
					workflowExecutionId,null);
			
			return;
		}else{
			log.info("checkCorpusHasFilesToProcess->" + toBeProcessed.size());
		}
	}
	
	private static class WorkflowExecution {
		private String executionId, workflowId, invocationId, corpusId;

		private ExecutionStatus status;

		protected WorkflowExecution(String executionId) {
			this.executionId = executionId;
		}

		protected String getInvocationId() {
			return invocationId;
		}

		protected void setInvocationId(String invocationId) {
			this.invocationId = invocationId;
		}

		protected String getExecutionId() {
			return executionId;
		}

		protected String getWorkflowId() {
			return workflowId;
		}

		protected void setWorkflowId(String workflowId) {
			this.workflowId = workflowId;
		}
		
		protected String getCorpusId() {
			return corpusId;
		}
		
		protected void setCorpusId(String corpusId) {
			this.corpusId = corpusId;
		}

		protected ExecutionStatus getExecutionStatus() {
			if (status != null) {
				status.setWorkflowId(workflowId);
				status.setCorpusID(corpusId);
			}
			
			return status;
		}

		protected void setExecutionStatus(ExecutionStatus status) {
			this.status = status;
			// result corpus 
			this.corpusId = status.getCorpusID();
		}
	}

	
	
	public static String resolveApplicationWorkflow(eu.openminted.registry.domain.Component application) {
		if (application == null) {
			throw new ServiceException("Application with id not found");
		}
		List<ResourceIdentifier> applicationIdentifiers = application.getComponentInfo().getIdentificationInfo().getResourceIdentifiers();
		Optional<ResourceIdentifier> workflowIdentifier = applicationIdentifiers.stream()
				.filter(identifier -> identifier.getResourceIdentifierSchemeName().equals(ResourceIdentifierSchemeNameEnum.OTHER)
						&& identifier.getSchemeURI().equals("workflowName")).findAny();
		if (!workflowIdentifier.isPresent()) {
			throw new ServiceException("No workflow name found in this application");
		}
		return workflowIdentifier.get().getValue();
	}
}
