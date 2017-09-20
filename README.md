# omtd-workflow-service
Workflow Service implementation using Galaxy as the execution engine

## Assumptions
- A document collection is passed into a workflow via the first input param called "Input Dataset Collection" found within the workflow
- A workflow should have no unset params (i.e. be a completely configured black box)
- no assumptions on file format are made by the workflow service it simply pass the files into Galaxy, runs the workflow, and retrieves the results.
- The ID of the workflow to run (retrieved from the metadata header) is assumed to be the name of the Galaxy workflow to run.
- the format of a corpus in the store is detailed in http://redmine.openminted.eu/projects/platform/wiki/Corpus_Specification
    
## Hints/Tips
- URLs and API keys are currently hardcoded into the test class, although they are specified in `application.properties` for normal use
- Archive ID for the new corpus containing the annotations is stored in the ExecutionStatus object for the completed workflow job

## How to configure
- You will need to install
  - a running Galaxy instance, and an API key to access it
  - a OMTD store instance (local file based is fine for testing)
- configuration details need to go into
  - application.properties for the main code
  - they are hardcoded into the test code (an unfortunate problem with spring injection)

