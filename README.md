# omtd-workflow-service
Workflow Service implementation using Galaxy as the execution engine

## Assumptions
- A document is passed into a workflow via the first input param called "Input Dataset" found within the workflow
  - this limits us to workflows that take a single dataset as input
- Regardless of how many outputs a workflow produces the last one is assumed to be the final output.
  - this means we skip intermediate results
  - but it also means that we loose output if, for some reason, a workflow produces multiple output files
- no assumptions on file format are made by the workflow service it simply pass the files into Galaxy, runs the workflow, and retrieves the results.
- The ID of the workflow to run (see below for an issue here) is assumed to be the name of the Galaxy workflow to run.
  - If such a workflow doesn't exist in Galaxy but the defintion file can be found on the workflow service classpath then it will be uploaded for use
    - motly useful for testing purposes
    - assumes that all tools needed for the workflow are properly installed within Galaxy

## Outstanding Issues
- Where inside a `eu.openminted.registry.domain.Component` is the ID of the component storred? The best I can currently find is `workflowJob.getWorkflow().getComponentInfo().getIdentificationInfo().getIdentifiers().get(0).getValue();` which can't possibly be correct, especially as `getIdentifiers()` returns a list so there is no guarantee of a single component ID using this approach.
  - fortunately this is easy to change once clarrified; one line in the impl and a little more in the tests where the ID of a workflow is set
- The format of a corpus within the OMTD store is, as yet, not fully defined other than the documents being storred within a zip file.
  - The code currently downloads the zip file and uses all files in a `documents` folder
  - reuslts are stored in a new archive in a `annotations` folder using the same filenames as were in the input
  - Code needs updating to
    - use the correct zip structures once this is finalized
    
## Hints/Tips
- for testing purposes if you use a file URL as the corpus ID then all documents within the directory pointed to by the URL are used for the corpus. This makes testing without an OMTD store instance easy
- URLs and API keys are currently hardcoded into the test class, although they are specified in `application.properties` for normal use
- Archive ID for the new corpus containing the annotations is stored in the ExecutionStatus object for the completed workflow job

## How to configure
- You will need to install
  - a running Galaxy instance, and an API key to access it
  - a OMTD store instance (local file based is fine for testing)
- configuration details need to go into
  - application.properties for the main code
  - they are hardcoded into the test code (an unfortunate problem with spring injection)
- To run the tests the Galaxy instance will need a workflow called ANNIE to be setup (what exactly it is doesn't matter)

