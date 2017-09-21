# omtd-workflow-service
Workflow Service implementation using Galaxy as the execution engine

## Assumptions
- A document collection is passed into a workflow via the first input parameter called "Input Dataset Collection" found within the workflow
- A workflow should have no unset paramaters (i.e. be a completely configured black box)
- no assumptions on file format are made by the workflow service it simply pass the files into Galaxy, runs the workflow, and retrieves the results.
- The ID of the workflow to run (retrieved from the metadata header) is assumed to be the name of the Galaxy workflow to run.
- the format of a corpus in the store is detailed in http://redmine.openminted.eu/projects/platform/wiki/Corpus_Specification
    
## Hints/Tips
- URLs and API keys are currently hard-coded into the test class, although they are specified in `application.properties` for normal use
- Archive ID for the new corpus containing the annotations is stored in the ExecutionStatus object for the completed workflow job

## How to configure
- You will need to install
  - a running Galaxy instance, and an API key to access it
  - a OMTD store instance (local file based is fine for testing)
- configuration details need to go into
  - application.properties for the main code
  - they are hard-coded into the test code (an unfortunate problem with spring injection)

## Further configuration to maximise performance
Uploading files directly into the Galaxy history via HTTP is exceedingly slow and is not adequate for our needs, especially once we start seeing corpora with upwards of a 1000 documents etc. To get around this issue the workflow service makes use of the FTP upload feature in Galaxy. Now while it is referred to as being for FTP upload this feature simply allows Galaxy to link documents in a given folder structure into a history for use by a workflow. While the documentation (https://galaxyproject.org/admin/config/upload-via-ftp/) explains how you would use this in conjunction with an FTP server running on the same machine as Galaxy this is not the only way the feature can be used.

Our approach uses the FTP feature to allow us to quickly add documents to the history using a shared file system that can be accessed by both the Galaxy instance and the workflow service. When a workflow is invoked the workflow service extracts the relevant corpora from the OMTD Store and unpacks the files onto the shared file system using the directory structure expected by Galaxy. These files are then linked into the Galaxy history without needing to be uploaded via the slow HTTP route.

To configure this feature (if not configured files will be uploaded via HTTP as before) you need to do a number of things.

- Create a directory on a shared file system that can be accessed (read and write) by both the Galaxy instance and the workflow service
- Edit the Galaxy configuration file `config/galaxy.ini` to set the `ftp_upload_dir` to point to the shared directory created above
- Add and configure the following new parameters to `application.properties` of the workflow service
  - `galaxy.ftp.dir` must point to the shared folder
  - `galaxy.user.email` must be set to the e-mail address registered in Galaxy for the user whose API key the workflow service uses to access Galaxy

Once configured in this way any workflows that are invoked will use this route to get documents into the Galaxy history rather than HTTP upload.

Note that it may, in future, be possible to make this even quicker by cutting out the workflow service entirely. Instead we could configure the OMTD Store so that on request it dumps a corpus directly into the shared file system in the same way as the workflow service now does, which would remove the need to zip up the corpus, transfer it from the VM running the store to the workflow service VM, and then unzipping it again. Instead the store could simply send a directory path to the workflow service pointing to the unpacked corpus on the shared file system and the workflow service could then simply enumerate the files and add them to the history without having to transfer them across the network.
