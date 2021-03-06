/**
 * DirectPubSub.java
 * 
 * This sample demonstrates:
 *  - Subscribing to a topic for direct messages.
 *  - Publishing direct messages to a topic.
 *  - Receiving messages with a message handler.
 *
 * This sample shows the basics of creating a context, creating a
 * session, connecting a session, subscribing to a topic, and publishing
 * direct messages to a topic. This is meant to be a very basic example, 
 * so there are minimal session properties and a message handler that simply 
 * prints any received message to the screen.
 * 
 * Although other samples make use of common code to perform some of the
 * most common actions, many of those common methods are explicitly
 * included in this sample to emphasize the most basic building blocks of
 * any application.
 * 
 * Copyright 2006-2018 Solace Corporation. All rights reserved.
 */

package com.solacesystems.jcsmp.samples.introsamples;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.lang.Runtime;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.samples.introsamples.common.ArgParser;
import com.solacesystems.jcsmp.samples.introsamples.common.SampleApp;
import com.solacesystems.jcsmp.samples.introsamples.common.SampleUtils;
import com.solacesystems.jcsmp.samples.introsamples.common.SessionConfiguration;
import com.solacesystems.jcsmp.samples.introsamples.common.SessionConfiguration.AuthenticationScheme;

public class DirectPubSubPublisher extends SampleApp {
	
	JCSMPSession session = null;
	SessionConfiguration conf = null;
	Topic topic = null;
	Topic replyTopic = null;
	int msgNum = 1000;
	int longLatencyThresholdinMS = 100;
	int longLatencyCount = 0;
	int msgCount = 0;
	int msgRate = 5;
	boolean verbose = false;
	
	XMLMessageConsumer cons = null;
    XMLMessageProducer prod = null;    

	public static final String binaryAttachment = "Hello World";    
    	
	public DirectPubSubPublisher() {
	}
    
	void printUsage(boolean secure) {
		String strusage = ArgParser.getCommonUsage(secure);
		System.out.println(strusage);
	    System.out.println("Extra arguments for this sample:");
	    System.out.println("\t -n \t the number of messages to publish (default " + msgNum + ")\n");
	    System.out.println("\t -lt \t long latency threshold in ms (default " + longLatencyThresholdinMS + ")\n");
	    System.out.println("\t -r \t publish message rate (default " + msgRate + ")\n");
	    System.out.println("\t -v \t verbose (default " + verbose + ")\n");
	}

	// The message handler is invoked for each Direct message received
	// by the Session.
	//
    // Message handler code is executed within the API thread, which means
    // that it should deal with the message quickly or queue the message
    // for further processing in another thread.
	//
	// Note: In other samples, a common message handler is used. However, 
	// to emphasize this programming paradigm, the message 
	// receive handler is directly included in this sample.
	class SubMessageHandler implements XMLMessageListener {
		
		public SubMessageHandler() {
		}
		
		public void onException(JCSMPException exception) {
			System.err.println("Error occurred, printout follows.");
			exception.printStackTrace();
		}

		public void onReceive(BytesXMLMessage msg) {
			//System.out.println("Received message:");
			
			long diff = System.currentTimeMillis() - msg.getAttachmentByteBuffer().getLong();
			if(verbose)
			{
				System.out.println("Round trip latency is: " + diff + "ms");
			}
			
			msgCount++;
			if (diff > longLatencyThresholdinMS)
			{
			    Date date = new Date();
			    DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			    String stringDate = sdf.format(date);
				System.out.println("WARNING: Round trip latency is LONG: " + diff + "ms at " + stringDate);
				longLatencyCount++;
			}
		}
	}
	
	public static void main(String[] args) {
		DirectPubSubPublisher directPubSub = new DirectPubSubPublisher();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				System.out.println("Total number of messages published " + directPubSub.msgCount);
				System.out.println("Number of messages has latency longer than " + directPubSub.longLatencyThresholdinMS + "ms :" + directPubSub.longLatencyCount);
			}
		});
		
		directPubSub.run(args);
	}
	
	void run(String[] args) {
		
		
		// Parse command-line arguments.
		ArgParser parser = new ArgParser();
		if (parser.parse(args) != 0) {
			printUsage(parser.isSecure());
		} else {
			conf = parser.getConfig();
		}
		if (conf == null)
			finish(1);

	       Map<String,String> extraArguments = conf.getArgBag();
	       if (extraArguments.containsKey("-n")) {
	    	   msgNum = Integer.parseInt(extraArguments.get("-n"));
	       }
	       if (extraArguments.containsKey("-lt")) {
	    	   longLatencyThresholdinMS = Integer.parseInt(extraArguments.get("-lt"));
	       }
	       if (extraArguments.containsKey("-r")) {
	    	   msgRate = Integer.parseInt(extraArguments.get("-r"));
	       }
	       if (extraArguments.containsKey("-v")) {
	    	   verbose = true;
	       }
		
		// Create a new Session. The Session properties are extracted from the
		// SessionConfiguration that was populated by the command line parser.
		//
		// Note: In other samples, a common method is used to create the Sessions.
		// However, to emphasize the most basic properties for Session creation,
		// this method is directly included in this sample.
		try {
			// Create session from JCSMPProperties. Validation is performed by
			// the API, and it throws InvalidPropertiesException upon failure.
			System.out.println("About to create session.");
			System.out.println("Configuration: " + conf.toString());			
			
			JCSMPProperties properties = new JCSMPProperties();
			
			properties.setProperty(JCSMPProperties.HOST, conf.getHost());
			properties.setProperty(JCSMPProperties.USERNAME, conf.getRouterUserVpn().get_user());
			
			if (conf.getRouterUserVpn().get_vpn() != null) {
				properties.setProperty(JCSMPProperties.VPN_NAME, conf.getRouterUserVpn().get_vpn());
			}
			
			properties.setProperty(JCSMPProperties.PASSWORD, conf.getRouterPassword());
	        
			// With reapply subscriptions enabled, the API maintains a
			// cache of added subscriptions in memory. These subscriptions
			// are automatically reapplied following a channel reconnect.
			properties.setBooleanProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);

	        // Disable certificate checking
	        properties.setBooleanProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, false);

	        if (conf.getAuthenticationScheme().equals(AuthenticationScheme.BASIC)) {
	            properties.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_BASIC);   
	        } else if (conf.getAuthenticationScheme().equals(AuthenticationScheme.KERBEROS)) {
	            properties.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_GSS_KRB);   
	        }

	        // Customer parameters
	        properties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, 255);
	        properties.setBooleanProperty(JCSMPProperties.PUB_MULTI_THREAD, false);
	        properties.setProperty(JCSMPProperties.ACK_EVENT_MODE, JCSMPProperties.SUPPORTED_ACK_EVENT_MODE_WINDOWED);
	        
	        // Channel properties
	        JCSMPChannelProperties cp = (JCSMPChannelProperties) properties
				.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
	        
			if (conf.isCompression()) {
				// Compression is set as a number from 0-9 where 0 means "disable
				// compression" and 9 means max compression. The default is no
				// compression.
				// Selecting a non-zero compression level auto-selects the
				// compressed SMF port on the appliance, as long as no SMF port is
				// explicitly specified.
				cp.setCompressionLevel(9);
			}
			
			cp.setConnectRetries(5);
			
			session =  JCSMPFactory.onlyInstance().createSession(properties);			
			
		} catch (InvalidPropertiesException ipe) {
			System.err.println("Error during session creation: ");
			ipe.printStackTrace();
			finish(1);
		}

		try {
			
			// Acquire a message consumer and open the data channel to the appliance.
			System.out.println("About to connect to appliance.");
	        session.connect();
			cons = session.getMessageConsumer(new SubMessageHandler());			
			
			// Use a Topic subscription.
			replyTopic = JCSMPFactory.onlyInstance().createTopic(SampleUtils.SAMPLE_TOPIC + "Reply");
			System.out.printf("Setting topic subscription '%s'...\n", replyTopic.getName());
			session.addSubscription(replyTopic);
			System.out.println("Connected!");

			// Receive messages.
			cons.start();
			
			// Acquire a message producer.
			prod = session.getMessageProducer(new PrintingPubCallback());
			topic = JCSMPFactory.onlyInstance().createTopic(SampleUtils.SAMPLE_TOPIC);
			
			// Send 10 messages.
			for (int msgsSent = 0; msgsSent < msgNum; ++msgsSent) {
				
				XMLMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
				//msg.writeAttachment(SampleUtils.attachmentText.getBytes());
				byte[] bytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(System.currentTimeMillis()).array();
				msg.writeAttachment(bytes);
				msg.setDeliveryMode(DeliveryMode.DIRECT);
				prod.send(msg, topic);

                // Wait 1 second between messages. This also provides sufficient 
                // time for the final message to be received.
				Thread.sleep(1000/msgRate);
			}
			
			// Stop the consumer and remove the subscription.
			cons.stop();			
			session.removeSubscription(replyTopic);

			System.out.println("Total number of messages published " + msgCount);
			System.out.println("Number of messages has latency longer than " + longLatencyThresholdinMS + "ms :" + longLatencyCount);
			finish(0);
		} catch (Exception ex) {
			// Normally, we would differentiate the handling of various exceptions, but
			// to keep this sample simple, all exceptions
			// are handled in the same way.
			System.err.println("Encountered an Exception: " + ex.getMessage());
			ex.printStackTrace(System.err);
			System.out.println("Total number of messages published " + msgCount);
			System.out.println("Number of messages has latency longer than " + longLatencyThresholdinMS + "ms :" + longLatencyCount);
			finish(1);
		} 
	}
	
	protected void finish(final int status) {
		if (cons != null) {
			cons.close();
		}
		
		if (session != null) {
			session.closeSession();
		}
		
		System.exit(status);
	}	
}
