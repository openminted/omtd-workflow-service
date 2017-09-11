package eu.openminted.workflow.service;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.stereotype.Component;

import com.github.jmchilton.blend4j.galaxy.beans.OutputDataset;

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
public class WorkflowServiceImpl2 implements WorkflowService {

	private static final Logger log = LoggerFactory.getLogger(WorkflowServiceImpl2.class);
	
	private Galaxy galaxy;	

	private static Map<String, ExecutionStatus> statusMonitor = new HashMap<String, ExecutionStatus>();
	
	//@Autowired
	MessageServicePublisher messageServicePublisher;
		
	//@Autowired
	MessageServiceSubscriber messageServiceSubscriber;
	
	// these should probably both be set via injection
	@Value("${galaxy.url}")
	String galaxyInstanceUrl;

	@Value("${galaxy.apiKey}")
	String galaxyApiKey;

	@Value("${store.endpoint}")
	String storeEndpoint;

	public WorkflowServiceImpl2(){
		log.info("Implementation:" + WorkflowServiceImpl2.class.getName());
	}
	
	public WorkflowServiceImpl2(MessageServicePublisher messageServicePublisher, MessageServiceSubscriber messageServiceSubscriber){
		log.info("Implementation:" + WorkflowServiceImpl2.class.getName());
		
		this.messageServicePublisher = messageServicePublisher;
		this.messageServiceSubscriber = messageServiceSubscriber;
		
		// Not the appropriate topics (used for testing).
		this.messageServiceSubscriber.addTopic(TopicsRegistry.workflowsExecution);
		this.messageServiceSubscriber.addTopic(TopicsRegistry.workflowsExecutionCompleted);
	}
	
	@Override
	public String execute(WorkflowJob workflowJob) throws WorkflowException {
		log.info("execute started");		
		
		initConnectionToGalaxy();
		
		final String workflowExecutionId = UUID.randomUUID().toString();
		updateStatus(new ExecutionStatus(Status.PENDING), workflowExecutionId, TopicsRegistry.workflowsExecution);

		log.info("Starting workflow execution " + workflowExecutionId + " using Galaxy instance at "
				+ galaxyInstanceUrl);

		Runnable runner = new Runnable() {

			public void run() {
				//if (!shouldContinue(workflowExecutionId)){
				//	log.info("shouldContinue false");
				//	//return;					
				//}

				updateStatus(new ExecutionStatus(Status.RUNNING), workflowExecutionId, TopicsRegistry.workflowsExecution);

				StoreRESTClient storeClient = new StoreRESTClient(storeEndpoint);

				/**
				 * get workflow from job, it's a Component instance so will then
				 * need to get it out of there. question is what does that
				 * object contain and where is the workflow file actually
				 * stored?
				 **/

				String workflowID = workflowJob.getWorkflow().getMetadataHeaderInfo().getMetadataRecordIdentifier()
						.getValue();

				// make sure we have the workflow we want to run and get it's
				// details
				final String workflowIDInGalaxy = galaxy.ensureHasWorkflow(workflowID);

				if (workflowIDInGalaxy == null) {
					log.info("Unable to locate workflow: " + workflowID);
					
					updateStatus(new ExecutionStatus(new WorkflowException("Unable to locate named workflow")), workflowExecutionId, TopicsRegistry.workflowsExecution);
					return;
				}

				log.info("Workflow ID: " + workflowIDInGalaxy);

				/**
				 * download the corpus from the OMTD-STORE using the REST
				 * client. This should get us a folder but not sure the format
				 * of the contents has been fixed yet
				 **/
												
				String corpusId = workflowJob.getCorpusId();
				
				String historyId = galaxy.createHistory();
				final List<String> ids = new ArrayList<String>();
				final ArrayList<File> filesList = new ArrayList<File>();
				
				if (!corpusId.startsWith("file:")) {
					File corpusZip;
					try {
						corpusZip = File.createTempFile("corpus", ".zip");
						String corpusZipFileName = corpusZip.getAbsolutePath();
						corpusZip.delete();
						log.info("download:" + corpusId + " to " + corpusZipFileName + " from " + storeEndpoint);
						StoreResponse storeResponse = storeClient.downloadArchive(corpusId, corpusZipFileName);
						
						if(!storeResponse.getResponse().startsWith("true")){
							throw new IOException("Problem on downloading from STORE.");
						}
						// create a new history for this run and upload the input
						// files to it					
						log.info("corpusZipFileName:" + corpusZipFileName);
						log.info("corpusId:" + corpusId);
						//log.info(corpusZip.toURI());
						
					} catch (IOException e) {
						log.info("Unable to retrieve specified corpus with ID " + corpusId, e);
						
						updateStatus(new ExecutionStatus(e), workflowExecutionId, TopicsRegistry.workflowsExecution);						
						return;
					}

					try (FileSystem zipFs = FileSystems
							.newFileSystem(new URI("jar:" + corpusZip.getAbsoluteFile().toURI()), new HashMap<>());) {

						Path pathInZip = zipFs.getPath("/" + corpusId, "fulltext");

						Files.walkFileTree(pathInZip, new SimpleFileVisitor<Path>() {
							@Override
							public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs)
									throws IOException {

								// create a tmp file to hold the file from the
								// zip while we upload it to Galaxy
								Path tmpFile = null;

								try {
									log.info("Creating temp file:" + filePath.getFileName().toString());									
									tmpFile = Files.createTempFile(null, filePath.getFileName().toString());
									tmpFile = Files.copy(filePath, tmpFile, StandardCopyOption.REPLACE_EXISTING);
									log.info("Uploading..." + tmpFile.toFile().getAbsolutePath());
									OutputDataset dataset = galaxy.uploadFileToHistory(historyId, tmpFile.toFile());
									ids.add(dataset.getId());
									
									File f = tmpFile.toFile();
									filesList.add(f);

									return FileVisitResult.CONTINUE;

								} finally {
									//if (tmpFile != null)
									//	Files.delete(tmpFile);
								}

							}
						});
						

					} catch (IOException | URISyntaxException e) {
						log.error("Unable to upload corpus to Galaxy history", e);
						
						updateStatus(new ExecutionStatus(e), workflowExecutionId, TopicsRegistry.workflowsExecution);
						return;
					}
				} else {
					
					try {
						final File inputDir = toFile(new URL(corpusId));

						for (File f : inputDir.listFiles()) {
							OutputDataset dataset = galaxy.uploadFileToHistory(historyId, f);
							ids.add(dataset.getId());
							
							filesList.add(f);
						}

						galaxy.waitForHistory(historyId);

					} catch (InterruptedException | MalformedURLException e) {
						log.error("Unable to upload corpus to Galaxy history", e);
						
						updateStatus(new ExecutionStatus(e), workflowExecutionId, TopicsRegistry.workflowsExecution);
						return;
					}
				}

				Path outputDir = null;

				try {
					outputDir = Files.createTempDirectory("omtd-workflow-output");
				} catch (IOException e) {
					log.error("Unable to create annotations output dir", e);
					
					updateStatus(new ExecutionStatus(e), workflowExecutionId, TopicsRegistry.workflowsExecution);
					return;
				}
				
				boolean error = false;
				
				try{
					galaxy.runWorkflow(workflowID, workflowIDInGalaxy, historyId, ids, filesList, outputDir.toFile().getAbsolutePath() + "/");	
				}catch(Exception e){
					e.printStackTrace();
					log.debug("error",e);
					error = true;
				}	
				
				try {
					if (corpusId.startsWith("file:")) {
						File corpusDir = toFile(new URL(corpusId));
						Path corpusZip = Paths.get(corpusDir.getName() + ".zip");
						log.info(corpusDir.getName() + "\t" + corpusZip);
						pack(outputDir, corpusZip);
						
						updateStatus(new ExecutionStatus(corpusZip.toUri().toString()), workflowExecutionId, TopicsRegistry.workflowsExecution);
					} else {
						String archiveID = uploadArchive(storeClient, outputDir);
						
						updateStatus(new ExecutionStatus(archiveID), workflowExecutionId, TopicsRegistry.workflowsExecutionCompleted);
					}
				} catch (IOException e) {
					log.info("unable to store workflow results", e);
					
					updateStatus(new ExecutionStatus(e), workflowExecutionId, TopicsRegistry.workflowsExecution);
					error = true;
					//return;
				}				
									
				if(error){
					updateStatus(new ExecutionStatus(Status.FAILED), workflowExecutionId, TopicsRegistry.workflowsExecution);
				}
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

	private void initConnectionToGalaxy(){
		if(galaxy == null){
			galaxy = new Galaxy(galaxyInstanceUrl, galaxyApiKey);
			log.info("Connected to Galaxy");	
		}
	}
	
	private void updateStatus(ExecutionStatus executionStatus, String workflowExecutionId, String topic){		
		try{
			String status = executionStatus.getStatus().toString();
			statusMonitor.put(workflowExecutionId, executionStatus);
			
			log.info("updateStatus:" + topic + "-->" + status);
			
			WorkflowExecutionStatusMessage msg = new WorkflowExecutionStatusMessage(); 
			msg.setWorkflowExecutionID(workflowExecutionId);
			msg.setWorkflowStatus(status);
			
			if(status.equalsIgnoreCase(ExecutionStatus.Status.FINISHED.toString())){
				msg.setResultingCorpusID(executionStatus.getCorpusID());
			}
			
			messageServicePublisher.publishMessage(topic, msg);
			log.info("updateStatus:" + topic + "-->" + status + " DONE");
		}catch(Exception e){
			e.printStackTrace();
			log.debug("error", e);
		}
	}
	
	private boolean shouldContinue(String workflowExecutionId) {
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

		statusMonitor.put(workflowExecutionId, new ExecutionStatus(Status.CANCELED));
	}

	@Override
	public void pause(String workflowExecutionId) throws WorkflowException {
		if (!statusMonitor.containsKey(workflowExecutionId))
			return;

		statusMonitor.put(workflowExecutionId, new ExecutionStatus(Status.PAUSED));
	}

	@Override
	public void resume(String workflowExecutionId) throws WorkflowException {
		if (!statusMonitor.containsKey(workflowExecutionId))
			return;

		if (!statusMonitor.get(workflowExecutionId).getStatus().equals(Status.PAUSED))
			return;

		statusMonitor.put(workflowExecutionId, new ExecutionStatus(Status.RUNNING));
	}

	@Override
	public ExecutionStatus getExecutionStatus(String workflowExecutionId) throws WorkflowException {
		if (!statusMonitor.containsKey(workflowExecutionId))
			throw new WorkflowException("Unknown Workflow Execution ID");

		return statusMonitor.get(workflowExecutionId);
	}
	
	@Override
	public void delete(eu.openminted.registry.domain.Component arg0) throws WorkflowException {
				
	}

	private static File toFile(URL url) {
		try {
			return new File(url.toURI());
		} catch (URISyntaxException e) {
			return new File(url.getPath());
		}
	}

	private static void pack(Path sourceDir, Path zipFile) throws IOException {

		try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(zipFile))) {

			Files.walk(sourceDir).filter(path -> !Files.isDirectory(path)).forEach(path -> {
				ZipEntry zipEntry = new ZipEntry(sourceDir.relativize(path).toString());
				try {
					zs.putNextEntry(zipEntry);
					zs.write(Files.readAllBytes(path));
					zs.closeEntry();
				} catch (Exception e) {
					System.err.println(e);
				}
			});
		}
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
		SpringApplication.run(WorkflowServiceImpl2.class, args);
	}
}
