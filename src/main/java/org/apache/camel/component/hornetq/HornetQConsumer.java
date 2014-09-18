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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.impl.DefaultMessage;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.MessageHandler;
import org.apache.commons.codec.binary.Base64;

/**
 * The HornetQComponent consumer.
 */
public class HornetQConsumer extends DefaultConsumer implements MessageHandler {

    private final HornetQEndpoint endpoint;

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
        System.out.println("doStart called");

        ClientConsumer consumer = this.endpoint.getClientConsumer();
        consumer.setMessageHandler(this);

    }

    @Override
    protected void doStop() throws Exception {
        System.out.println("doStop called");
    }

    @Override
    public void onMessage(ClientMessage message) {

        String primaryKey = message.getStringProperty("PRIMARYKEY");
        String replicationNo = message.getStringProperty("EVENTID");
        String opcode = message.getStringProperty("OPCODE");
        System.out.println(String.format("Message Header value for replication no: %s  primary key: %s", replicationNo, primaryKey));
        if (message.getBodyBuffer().readable()) {
            String msgBody = message.getBodyBuffer().readString();
            JsonElement jelement = new JsonParser().parse(msgBody);
            JsonObject  jobject = jelement.getAsJsonObject();
            jobject = jobject.getAsJsonObject("EXTENDED_DATA");
            System.out.println("EXTENDED_DATA = " + jobject);
 //           Map<String, String> msgMap = GsonJsonParser.deserializeMessageBodyAsJavaMap(msgBody);
            System.out.println(msgBody);

            if (jobject != null) {
                byte[] bytes = Base64.decodeBase64(jobject.getAsString());
                System.out.print("decoded extended data: ");
                System.out.print(bytesToHex(bytes));
                System.out.println();
            } else {
                System.out.println(String.format("Did not find and EXTENDED_DATA - opcode:%s", opcode));
            }
        }
        try {
            message.acknowledge();
        } catch (HornetQException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Exchange exchange = endpoint.createExchange();
        Message m = new DefaultMessage();
        m.setBody(message);

        try {
            getProcessor().process(exchange);
        } catch (Exception e) {
            exchange.setException(e);
        }
    }

    private String bytesToHex(byte[] bytes) {
        final char[] hexArray = "0123456789ABCDEF".toCharArray();
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

}
