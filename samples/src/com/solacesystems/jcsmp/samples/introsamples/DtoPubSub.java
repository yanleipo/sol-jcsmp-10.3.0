/**
 * DtoPubSub.java
 *
 * This sample demonstrates:
 *  1. Publishing a message using Deliver-To-One (DTO)
 *  2. Subscribing to a topic using DTO override to receive all messages.
 *
 * In this sample, we create three sessions to a SolOS-TR appliance:
 *  session     - Publish messages to the topic with the DTO flag set.
 *              - Subscribe to the topic with DTO override set.
 *  dtoSession1 - Subscribe to the topic.
 *  dtoSession2 - Subscribe to the topic.
 *
 * With the DTO flag set on messages being published, the appliance delivers
 * messages to 'dtoSession1' and 'dtoSession2' in a round robin manner. In 
 * addition to delivering the message to either 'dtoSession1' or 
 * 'dtoSession2', the appliance delivers all messages to 'session'.  
 *
 * Note: 'session' is not part of the round robin to receive DTO messages
 * because its subscription uses DTO-override.
 * 
 * Copyright 2009-2018 Solace Corporation. All rights reserved.
 */

package com.solacesystems.jcsmp.samples.introsamples;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.TopicProperties;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.samples.introsamples.common.ArgParser;
import com.solacesystems.jcsmp.samples.introsamples.common.SampleApp;
import com.solacesystems.jcsmp.samples.introsamples.common.SampleUtils;
import com.solacesystems.jcsmp.samples.introsamples.common.SessionConfiguration;
import com.solacesystems.jcsmp.statistics.StatType;

public class DtoPubSub extends SampleApp {
	
	Consumer cons = null;
    XMLMessageProducer prod = null;
	SessionConfiguration conf = null;
	
	JCSMPSession dtoSession1 = null;
	JCSMPSession dtoSession2 = null;
	Consumer dtoCons1 = null;
	Consumer dtoCons2 = null;
	

	/**
	 * This helper method creates the three Sessions required by this sample. The Session
	 * properties for all three Sessions are derived from the command line arguments.
	 * 
	 * @param args The command line arguments
	 */
	void createSessions(String[] args) {
		ArgParser parser = new ArgParser();

		if (parser.parse(args) == 0)
			conf = parser.getConfig();
		else
			printUsage(parser.isSecure());
        session = SampleUtils.newSession(conf, new PrintingSessionEventHandler(),null);
		dtoSession1 = SampleUtils.newSession(conf, new PrintingSessionEventHandler(),null);
		dtoSession2 = SampleUtils.newSession(conf, new PrintingSessionEventHandler(),null);
	}
	
	/**
	 * Prints instructions on how to use this sample.
	 */
	void printUsage(boolean secure) {
		String strusage = ArgParser.getCommonUsage(secure);
		System.out.println(strusage);
		finish(1);
	}
	
	/**
	 * Default constructor.
	 */
	public DtoPubSub() {
	}

	/**
	 * The main method.
	 * @param args The command line arguments
	 */
	public static void main(String[] args) {
		DtoPubSub dtoPubSub = new DtoPubSub();
		dtoPubSub.run(args);
	}
	
	/**
     * Message handler class that takes a session name as a parameter to the
     * constructor and outputs this Session name when a message is received.
	 */
	class DtoPrintingMessageHandler extends SampleApp.PrintingMessageHandler {
		private String sessionName;
		
		public DtoPrintingMessageHandler(String sessionName) {
			this.sessionName = sessionName;
		}
		
		public void onReceive(BytesXMLMessage msg) {
			// This call (getSequenceNumber) accesses custom header data and could
			// impact performance.
			System.out.println(sessionName + " received message. (seq# " + msg.getSequenceNumber() + ")");
			System.out.flush();
		}
	}
	
	/**
	 * Runs through the sample.
	 * @param args The command line arguments.
	 */
	void run(String[] args) {
		
		createSessions(args);

		try {
			System.out.println("About to connect to appliance.");
	        session.connect();
			prod = session.getMessageProducer(new PrintingPubCallback());
			printRouterInfo();
			
			// Create two Topics for the Sessions subscriptions. The only 
			// difference between the Topics is whether DTO override has been
			// enabled.
			TopicProperties tprop = new TopicProperties().setName(SampleUtils.SAMPLE_TOPIC);
			Topic myTopic = JCSMPFactory.onlyInstance().createTopic(tprop);
			
			tprop.setRxAllDeliverToOne(true);
			Topic myTopicDeliverAlways = JCSMPFactory.onlyInstance().createTopic(tprop);
			
			// Add the subscriptions to each Session.
			session.addSubscription(myTopicDeliverAlways);
			dtoSession1.addSubscription(myTopic);
			dtoSession2.addSubscription(myTopic);
			
			// Acquisition of consumers.
			cons = session.getMessageConsumer(new DtoPrintingMessageHandler("DTO Override Session"));
			cons.start();
			dtoCons1 = dtoSession1.getMessageConsumer(new DtoPrintingMessageHandler("DTO Session 1"));
			dtoCons1.start();
			dtoCons2 = dtoSession2.getMessageConsumer(new DtoPrintingMessageHandler("DTO Session 2"));
			dtoCons2.start();
			
			System.out.println("Connected!");

			// Send 10 messages.
			for (int i = 0; i < 10; ++i) {
				BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
				msg.writeBytes(SampleUtils.xmldoc.getBytes());
				msg.setDeliveryMode(DeliveryMode.DIRECT); // Set message delivery mode
				msg.setDeliverToOne(true); // Set the deliver to one (DTO) flag

				// This call accesses custom header data and can impact performance.
				msg.setSequenceNumber(i + 1);

				prod.send(msg, myTopic);
				System.out.println("Message " + (i + 1) + " sent.");
				Thread.sleep(500);
			}
			
			Thread.sleep(2000);
			cons.stop();
			dtoCons1.stop();
			dtoCons2.stop();
			finish(0);
		} catch (JCSMPException ex) {
			System.err.println("Encountered a JCSMPException, closing consumer channel... " + ex.getMessage());
			// Possible causes: 
			// - Authentication error: invalid username/password 
			// - Provisioning error: unable to add subscriptions from CSMP
			// - Invalid or unsupported properties specified
			if (cons != null) {
				cons.close();
				// At this point the consumer handle is unusable, a new one should be created 
				// by calling cons = session.getMessageConsumer(...) if the application 
				// logic requires the consumer channel to remain open.
			}
			if (dtoCons1 != null) {
				dtoCons1.close();
			}
			if (dtoCons2 != null) {
				dtoCons2.close();
			}
			finish(1);
		} catch (Exception ex) {
			System.err.println("Encountered an Exception... " + ex.getMessage());
			finish(1);
		}
	}
	
	/**
	 * Prints a summary of the sent and received messages (if any), closes the 
	 * sessions, and exits.
	 */
	@Override
	protected void finish(final int status) {
		
		// Print a summary of messages sent and received.
		if ((session != null) && (dtoSession1 != null) && (dtoSession2 != null)) {
			long msgSent = session.getSessionStats().getStat(StatType.TOTAL_MSGS_SENT);
			long msgReceived = session.getSessionStats().getStat(StatType.TOTAL_MSGS_RECVED) +
				dtoSession1.getSessionStats().getStat(StatType.TOTAL_MSGS_RECVED)+
				dtoSession2.getSessionStats().getStat(StatType.TOTAL_MSGS_RECVED);
			System.out.println("Number of messages sent: " + msgSent);
			System.out.println("Number of messages received: " + msgReceived);
		}
		
		// Close the Sessions.
		if (session != null) {
			session.closeSession();
		}
		if (dtoSession1 != null) {
			dtoSession1.closeSession();
		}
		if (dtoSession2 != null) {
			dtoSession2.closeSession();
		}
		
		System.exit(status);
	}
}
