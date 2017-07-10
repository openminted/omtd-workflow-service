package eu.openminted.workflow.service;

import java.io.File;
import java.io.IOException;
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

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;

import com.github.jmchilton.blend4j.galaxy.beans.OutputDataset;

import eu.openminted.store.common.StoreResponse;
import eu.openminted.store.restclient.StoreRESTClient;
import eu.openminted.workflow.api.ExecutionStatus;
import eu.openminted.workflow.api.ExecutionStatus.Status;
import eu.openminted.workflow.api.WorkflowException;
import eu.openminted.workflow.api.WorkflowJob;
import eu.openminted.workflow.api.WorkflowService;

//@RestController
//@EnableAutoConfiguration
@Component
public class WorkflowServiceImpl2 implements WorkflowService {

	private Galaxy galaxy;
	private static Logger log = Logger.getLogger(WorkflowServiceImpl2.class);

	private static Map<String, ExecutionStatus> status = new HashMap<String, ExecutionStatus>();

	// these should probably both be set via injection
	@Value("${galaxy.url}")
	String galaxyInstanceUrl;

	@Value("${galaxy.apiKey}")
	String galaxyApiKey;

	@Value("${store.endpoint}")
	String storeEndpoint;

	@Value("${galaxy.scriptsPath}")
	String galaxyScriptsPath;
	
	@RequestMapping("/")
	String home() {
		return "omtd-workflow-service for <a href=\"" + galaxyInstanceUrl + "\">galaxy</a>";
	}

	public WorkflowServiceImpl2(){
		log.info(WorkflowServiceImpl2.class.getName());
	}
	
	public static void main(String[] args) throws Exception {
		SpringApplication.run(WorkflowServiceImpl2.class, args);
	}

	@SuppressWarnings("unused")
	@Override
	public String execute(WorkflowJob workflowJob) throws WorkflowException {
		
		log.info("execute started");		
		galaxy = new Galaxy(galaxyInstanceUrl, galaxyApiKey);
		log.info("connected to Galaxy");
		
		final String workflowExecutionId = UUID.randomUUID().toString();
		status.put(workflowExecutionId, new ExecutionStatus(Status.PENDING));

		log.info("Starting workflow execution " + workflowExecutionId + " using Galaxy instance at "
				+ galaxyInstanceUrl);

		Runnable runner = new Runnable() {

			public void run() {
				//if (!shouldContinue(workflowExecutionId)){
				//	log.info("shouldContinue false");
				//	//return;					
				//}

				status.put(workflowExecutionId, new ExecutionStatus(Status.RUNNING));

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
				final String testWorkflowId = galaxy.ensureHasWorkflow(workflowID);

				if (testWorkflowId == null) {
					log.info("Unable to locate workflow: " + workflowID);
					status.put(workflowExecutionId,
							new ExecutionStatus(new WorkflowException("Unable to locate named workflow")));
					return;
				}

				log.info("Workflow ID: " + testWorkflowId);

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
						
						// create a new history for this run and upload the input
						// files to it					
						log.info("corpusZipFileName:" + corpusZipFileName);
						log.info("corpusId:" + corpusId);
						//log.info(corpusZip.toURI());
						
					} catch (IOException e) {
						log.info("Unable to retrieve specified corpus with ID " + corpusId, e);
						status.put(workflowExecutionId, new ExecutionStatus(e));
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
						status.put(workflowExecutionId, new ExecutionStatus(e));
						return;
					}
				} else {
					/*
					try {
						final File inputDir = toFile(new URL(corpusId));

						for (File f : inputDir.listFiles()) {
							OutputDataset dataset = upload(instance, historyId, f);
							ids.put(dataset.getId(), f.getName());
						}

						waitForHistory(instance.getHistoriesClient(), historyId);

					} catch (InterruptedException | MalformedURLException e) {
						log.error("Unable to upload corpus to Galaxy history", e);
						status.put(workflowExecutionId, new ExecutionStatus(e));
						return;
					}*/
				}

				Path outputDir = null;

				try {
					outputDir = Files.createTempDirectory("omtd-workflow-output");
				} catch (IOException e) {
					log.error("Unable to create annotations output dir", e);
					status.put(workflowExecutionId, new ExecutionStatus(e));
					return;
				}
				
				boolean error = false;
				
				try{
					galaxy.setScriptsPath(galaxyScriptsPath);
					galaxy.runWorkflow(workflowID, testWorkflowId, historyId, ids, filesList, outputDir.toFile().getAbsolutePath() + "/");	
				}catch(Exception e){
					log.info(e);
					error = true;
				}	
				
				try {
					if (corpusId.startsWith("file:")) {
						File corpusDir = toFile(new URL(corpusId));
						Path corpusZip = Paths.get(corpusDir.getName() + ".zip");
						log.info(corpusDir.getName() + "\t" + corpusZip);
						pack(outputDir, corpusZip);
						status.put(workflowExecutionId, new ExecutionStatus(corpusZip.toUri().toString()));
					} else {
						String archiveID = uploadArchive(storeClient, outputDir);
						status.put(workflowExecutionId, new ExecutionStatus(archiveID));
					}
				} catch (IOException e) {
					log.info("unable to store workflow results", e);
					status.put(workflowExecutionId, new ExecutionStatus(e));
					error = true;
					//return;
				}				
									
				if(error){
					status.put(workflowExecutionId, new ExecutionStatus(Status.FAILED));
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
		if (!status.containsKey(workflowExecutionId))
			return;

		status.put(workflowExecutionId, new ExecutionStatus(Status.CANCELED));
	}

	@Override
	public void pause(String workflowExecutionId) throws WorkflowException {
		if (!status.containsKey(workflowExecutionId))
			return;

		status.put(workflowExecutionId, new ExecutionStatus(Status.PAUSED));
	}

	@Override
	public void resume(String workflowExecutionId) throws WorkflowException {
		if (!status.containsKey(workflowExecutionId))
			return;

		if (!status.get(workflowExecutionId).getStatus().equals(Status.PAUSED))
			return;

		status.put(workflowExecutionId, new ExecutionStatus(Status.RUNNING));
	}

	@Override
	public ExecutionStatus getExecutionStatus(String workflowExecutionId) throws WorkflowException {
		if (!status.containsKey(workflowExecutionId))
			throw new WorkflowException("Unknown Workflow Execution ID");

		return status.get(workflowExecutionId);
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
}
