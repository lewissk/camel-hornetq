/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.camel.component.hornetq;

import java.util.HashMap;
import org.apache.camel.Consumer;
import org.apache.camel.EndpointConfiguration;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSession.QueueQuery;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a HornetQComponent endpoint.
 */
public class HornetQEndpoint extends DefaultEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(HornetQEndpoint.class);

    public HornetQEndpoint() {

    }

    public HornetQEndpoint(String uri, HornetQComponent component) {
        super(uri, component);

    }

    public HornetQEndpoint(String endpointUri) {
        super(endpointUri);

    }

    @Override
    protected void doStart() throws Exception {
        this.createClientConsumer();
    }

    @Override
    protected void doStop() throws Exception {
        hornetQClientSession.commit();
        hornetQClientSession.close();
        hornetQServerLocator.close();
    }

    @Override
    public Producer createProducer() throws Exception {
        return new HornetQProducer(this);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        System.out.println("called createConsumer " + processor.getClass().getName());
        if (hornetQClientConsumer == null) {
            LOG.debug("clientConsumer = " + hornetQClientConsumer);
            try {
                this.createClientConsumer();
            } catch (Throwable t) {
                LOG.error("Error in initialize client session", t);
            }
        }
        return new HornetQConsumer(this, processor);
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    /**
     *
     * @throws Exception
     */
    public void createClientConsumer() throws Exception {
        ClientSession cs = createHornetQClientSession();
        QueueQuery qq = cs.queueQuery(new SimpleString(queueName));
        if (!qq.isExists()) {
            cs.createQueue(new SimpleString(topicName), new SimpleString(queueName), true);
        }
        ClientConsumer cc = cs.createConsumer(queueName);

        cs.start();
        this.hornetQClientConsumer = cc;
    }

    private ClientSession createHornetQClientSession() throws Exception, HornetQException {
        if (hornetQClientSession == null) {
            ServerLocator locator = createHornetQServerLocator();
            // default values typically set up in the config file
            locator.setBlockOnNonDurableSend(true);
            locator.setBlockOnDurableSend(true);
            locator.setConnectionTTL(90000);
            locator.setCallTimeout(60000);
            locator.setReconnectAttempts(5);
            locator.setMinLargeMessageSize(10 * 1024);
            ClientSessionFactory factory = locator.createSessionFactory();
            hornetQClientSession = factory.createSession(username, password, false, true, true, false, 0);
        }
        return hornetQClientSession;
    }

    private ServerLocator createHornetQServerLocator() {
        if (this.hornetQServerLocator == null) {
            HashMap connectionProps = new HashMap<String, Object>();
            LOG.debug("ENDPoint URI = " + this.getEndpointUri());

            EndpointConfiguration conf = getEndpointConfiguration();
            connectionProps.put(TransportConstants.HOST_PROP_NAME, conf.getParameter(EndpointConfiguration.URI_HOST));
            connectionProps.put(TransportConstants.PORT_PROP_NAME, conf.getParameter(EndpointConfiguration.URI_PORT));
            connectionProps.put(TransportConstants.USE_NIO_PROP_NAME, "true");

            TransportConfiguration transportConfiguration = new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory", connectionProps);
            ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(transportConfiguration);
            this.hornetQServerLocator = locator;
        }
        return hornetQServerLocator;
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
        return hornetQClientConsumer;
    }

    private String password;
    private String username;
    private String queueName;
    private String topicName;
    private ClientSession hornetQClientSession;
    private ClientConsumer hornetQClientConsumer;
    private ServerLocator hornetQServerLocator;

}
