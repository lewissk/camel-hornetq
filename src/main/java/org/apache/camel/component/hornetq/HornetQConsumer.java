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


import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.impl.DefaultMessage;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.MessageHandler;
import org.apache.camel.spi.Synchronization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HornetQComponent consumer.
 */
public class HornetQConsumer extends DefaultConsumer implements MessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HornetQConsumer.class);
    private final HornetQEndpoint endpoint;
//    private ScheduledExecutorService scheduledExecutor;

    public HornetQConsumer(HornetQEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
    }

    @Override
    public HornetQEndpoint getEndpoint() {
        return (HornetQEndpoint) super.getEndpoint();
    }

    @Override
    protected void doStart() throws Exception {
        LOG.debug("doStart called");

        ClientConsumer consumer = this.endpoint.getClientConsumer();
        consumer.setMessageHandler(this);

        //startRobustConnectionMonitor();
        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        System.out.println("doStop called");
        ClientConsumer consumer = this.endpoint.getClientConsumer();
        consumer.close();
    }

    @Override
    public void onMessage(final ClientMessage message) {

        LOG.debug("Received Message = " + message);

        Exchange exchange = endpoint.createExchange();
        Message m = new DefaultMessage();
        m.setBody(message);
        exchange.setIn(m);

        try {
            getAsyncProcessor().process(exchange);
            exchange.addOnCompletion(new Synchronization() {

                @Override
                public void onComplete(Exchange exchange) {
                    try {
                        LOG.debug("Message acknowledged: " + exchange.getIn());
                        message.acknowledge();
                    } catch (HornetQException e) {
                        LOG.error("Could not acknowledge message", e);
                    }
                }

                @Override
                public void onFailure(Exchange exchange) {
                    LOG.error("Error processing message: " + message);
                }
            });
        } catch (Exception e) {
            exchange.setException(e);
        }
    }

//    private ScheduledExecutorService getExecutor() {
//        if (this.scheduledExecutor == null) {
//            scheduledExecutor = getEndpoint().getCamelContext().getExecutorServiceManager().newSingleThreadScheduledExecutor(this, "connectionPoll");
//        }
//        return scheduledExecutor;
//    }

//    private void startRobustConnectionMonitor() throws Exception {
//        Runnable connectionCheckRunnable = new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    checkConnection();
//                } catch (Exception e) {
//                    LOG.warn("Ignoring an exception caught in the connection poller thread.", e);
//                }
//            }
//
//        };
//
//        // background thread to detect and repair lost connections
//        getExecutor().scheduleAtFixedRate(connectionCheckRunnable, 1000000, 1000000, TimeUnit.SECONDS);
//    }
//
//    private void checkConnection() throws Exception {
//        System.out.println("check connection");
//        if (!connection.isConnected()) {
//            LOG.info("Attempting to reconnect to: {}", XmppEndpoint.getConnectionMessage(connection));
//            try {
//                connection.connect();
//            } catch (XMPPException e) {
//                LOG.warn(XmppEndpoint.getXmppExceptionLogMessage(e));
//            }
//        }
//    }

}
