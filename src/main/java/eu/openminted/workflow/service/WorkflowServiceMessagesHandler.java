package eu.openminted.workflow.service;

import javax.jms.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.openminted.messageservice.connector.MessagesHandler;

public class WorkflowServiceMessagesHandler implements MessagesHandler{

	private static final Logger log = LoggerFactory.getLogger(MessagesHandler.class);

	@Override
	public void handleMessage(Message msg) {
		log.info("handleMessage...");
	}

}
