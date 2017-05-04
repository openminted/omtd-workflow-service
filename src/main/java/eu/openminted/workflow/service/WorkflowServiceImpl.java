package eu.openminted.workflow.service;

import eu.openminted.workflow.api.*;

import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.stereotype.*;
import org.springframework.web.bind.annotation.*;

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
}
