/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.hornetq;

import java.util.HashMap;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSession.QueueQuery;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;

/**
 * Represents a HornetQComponent endpoint.
 */
public class HornetQEndpoint extends DefaultEndpoint {

    public HornetQEndpoint() {
    }

    public HornetQEndpoint(String uri, HornetQComponent component) {
        super(uri, component);
    }

    public HornetQEndpoint(String endpointUri) {
        super(endpointUri);
    }

    @Override
    public Producer createProducer() throws Exception {
        return new HornetQProducer(this);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        System.out.println("called createConsumer " + processor.getClass().getName());
        if(clientConsumer == null) {
            this.initializeClientSession();
        }
        return new HornetQConsumer(this, processor);
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
    
    
    private void initializeClientSession() throws Exception {
        HashMap connectionProps = new HashMap<String, Object>();
        System.out.println("ENDPoint URI = " + this.getEndpointUri());
        
        String[] hostAndPort;
        if(this.getEndpointUri() != null && (hostAndPort = this.getEndpointUri().split(":")).length > 0) {
            connectionProps.put(TransportConstants.HOST_PROP_NAME, "127.0.0.1");
            connectionProps.put(TransportConstants.PORT_PROP_NAME, "5445");
            connectionProps.put(TransportConstants.USE_NIO_PROP_NAME, "true");
        } else {
            //throw new Exception("Error in configuration: host and port not specified properly host:port port is necessary even if it is the default, " + this.getEndpointUri());
        }
        
        TransportConfiguration transportConfiguration = new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory", connectionProps);
        ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(transportConfiguration);
        // default values typically set up in the config file
        locator.setBlockOnNonDurableSend(true);
        locator.setBlockOnDurableSend(true);
        locator.setConnectionTTL(90000);
        locator.setCallTimeout(60000);
        locator.setReconnectAttempts(5);
        locator.setMinLargeMessageSize(10*1024);
    
        ClientSessionFactory factory = locator.createSessionFactory();
        
        ClientSession clientSession = factory.createSession(username, password, false, true, true, false, 0);
        QueueQuery qq = clientSession.queueQuery(new SimpleString(queueName));
        if (!qq.isExists()) {
            clientSession.createQueue(new SimpleString(topicName),new SimpleString(queueName), true);
        }
        ClientConsumer sub = clientSession.createConsumer(queueName);

        clientSession.start();
        this.clientConsumer = sub;
    }
    
    /**
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @param username the username to set
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return the queueName
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * @param queueName the queueName to set
     */
    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    /**
     * @return the topicName
     */
    public String getTopicName() {
        return topicName;
    }

    /**
     * @param topicName the topicName to set
     */
    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    /**
     * @return the clientConsumer
     */
    public ClientConsumer getClientConsumer() {
        return clientConsumer;
    }

    private String password;
    private String username;
    private String queueName;
    private String topicName;
    private ClientConsumer clientConsumer;
    
    
}
