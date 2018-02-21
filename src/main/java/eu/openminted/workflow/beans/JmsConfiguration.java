package eu.openminted.workflow.beans;

import eu.openminted.workflow.service.WorkflowServiceImpl;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageConverter;
import org.springframework.jms.support.converter.MessageType;

@Configuration
@PropertySource("classpath:messageService.properties")
@ComponentScan(basePackageClasses = {WorkflowServiceImpl.class})
public class JmsConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(JmsConfiguration.class);

    @Value("${workflows.editor}")
    private String workflowsEditor;

    @Value("${workflows.execution}")
    private String workflowsExecution;

    @Value("${workflows.completed}")
    private String workflowsExecutionCompleted;

    @Value("${registry.component.create}")
    private String registryComponentCreate;

    @Value("${registry.component.update}")
    private String registryComponentUpdate;

    @Value("${registry.component.delete}")
    private String registryComponentDelete;

    @Value("${messageService.host}")
    private String jmsHost;

    @Bean
    public ActiveMQConnectionFactory activeMQConnectionFactory() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        connectionFactory.setBrokerURL(jmsHost);
        logger.info("ActiveMQConnection Factory created for " + jmsHost);
        return connectionFactory;
    }

    @Bean // Serialize message content to json using TextMessage
    public MessageConverter jacksonJmsMessageConverter() {
        MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
        converter.setTargetType(MessageType.TEXT);
        converter.setTypeIdPropertyName("_type");
        return converter;
    }

    private DefaultJmsListenerContainerFactory getListenerContainerFactory(boolean b) {
        DefaultJmsListenerContainerFactory factory
                = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(activeMQConnectionFactory());
        factory.setPubSubDomain(b); // false is for queue
        factory.setMessageConverter(jacksonJmsMessageConverter());
        return factory;
    }

    @Bean
    public DefaultJmsListenerContainerFactory jmsQueueListenerContainerFactory() {
        return getListenerContainerFactory(false);
    }

    @Bean
    public DefaultJmsListenerContainerFactory jmsTopicListenerContainerFactory() {
        return getListenerContainerFactory(true);
    }

    private JmsTemplate getJmsTemplate(boolean b) {
        JmsTemplate template = new JmsTemplate();
        template.setConnectionFactory(activeMQConnectionFactory());
        template.setPubSubDomain(b); //false is for queue
        template.setMessageConverter(jacksonJmsMessageConverter());
        return template;
    }

    @Bean
    public JmsTemplate jmsQueueTemplate(){
        return getJmsTemplate(false);
    }

    @Bean
    public JmsTemplate jmsTopicTemplate(){
        return getJmsTemplate(true);
    }


    public String getWorkflowsEditor() {
        return workflowsEditor;
    }

    public void setWorkflowsEditor(String workflowsEditor) {
        this.workflowsEditor = workflowsEditor;
    }

    public String getWorkflowsExecution() {
        return workflowsExecution;
    }

    public void setWorkflowsExecution(String workflowsExecution) {
        this.workflowsExecution = workflowsExecution;
    }

    public String getWorkflowsExecutionCompleted() {
        return workflowsExecutionCompleted;
    }

    public void setWorkflowsExecutionCompleted(String workflowsExecutionCompleted) {
        this.workflowsExecutionCompleted = workflowsExecutionCompleted;
    }

    public String getRegistryComponentCreate() {
        return registryComponentCreate;
    }

    public void setRegistryComponentCreate(String registryComponentCreate) {
        this.registryComponentCreate = registryComponentCreate;
    }

    public String getRegistryComponentUpdate() {
        return registryComponentUpdate;
    }

    public void setRegistryComponentUpdate(String registryComponentUpdate) {
        this.registryComponentUpdate = registryComponentUpdate;
    }

    public String getRegistryComponentDelete() {
        return registryComponentDelete;
    }

    public void setRegistryComponentDelete(String registryComponentDelete) {
        this.registryComponentDelete = registryComponentDelete;
    }
}
