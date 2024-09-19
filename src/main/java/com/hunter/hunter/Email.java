package com.hunter.hunter;

/* Created Developer:Vijay Uniyal
* Company: Shubham Housing Pvt Ltd.
* Description:
* */

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Email implements Runnable {
	protected static final Logger logger = LoggerFactory.getLogger(Email.class);

	private String host;
	private String port;
	private String email;
	private String password;
	private String email_to;
	private String email_subject;
	private String email_body;
	private String filename;

	//
	public Email(String mailTo, String mailSubject, String mailBody, String filename, String host, String port,
			String email, String password) {
		this.email_to = mailTo;
		this.email_body = mailBody;
		this.email_subject = mailSubject;
		this.filename = filename;
		this.host = host;
		this.port = port;
		this.email = email;
		this.password = password;

	}

	public void run() {

		Properties props = new Properties();
		props.put("mail.smtp.host", host);
		props.put("mail.smtp.socketFactory.port", port);
		props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.port", port);
		props.put("mail.debug", "true");
		// get Session
		Session session = Session.getDefaultInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(email, password);
			}
		});

		try {
			MimeMessage message = new MimeMessage(session);
			message.setFrom(new InternetAddress(email));
			String[] recipientList = email_to.split(",");
			InternetAddress[] recipientAddress = new InternetAddress[recipientList.length];
			int counter = 0;
			for (String recipient : recipientList) {
				recipientAddress[counter] = new InternetAddress(recipient.trim());
				counter++;
			}
			message.setRecipients(Message.RecipientType.TO, recipientAddress);
			message.setRecipients(Message.RecipientType.BCC, "vijay.uniyal@shubham.co");

			message.setSubject(email_subject);
			if (filename == null) {
				message.setContent(email_body, "text/html; charset=utf-8");

			} else {
				BodyPart messageBodyPart1 = new MimeBodyPart();
				messageBodyPart1.setContent(email_body, "text/html; charset=utf-8");

				// 5) create Multipart object and add MimeBodyPart objects to this object
				Multipart multipart = new MimeMultipart();
				multipart.addBodyPart(messageBodyPart1);
				if (filename != null) {
					MimeBodyPart messageBodyPart2 = new MimeBodyPart();
					File file = new File(filename);
					if(file.exists())
					{
						DataSource source = new FileDataSource(file);
						messageBodyPart2.setDataHandler(new DataHandler(source));
						messageBodyPart2.setFileName(filename);
						multipart.addBodyPart(messageBodyPart2);
					}
					MimeBodyPart messageBodyPart3 = new MimeBodyPart();
					File filexls = new File(filename.replace(".xml", ".xlsx"));
					if(filexls.exists()) {
						DataSource sourcexls = new FileDataSource(filexls);
						messageBodyPart3.setDataHandler(new DataHandler(sourcexls));
						messageBodyPart3.setFileName(filename.replace(".xml", ".xlsx"));
						multipart.addBodyPart(messageBodyPart3);
					}
					
				}

				// 6) set the multiplart object to the message object
				message.setContent(multipart);
			}
			// message.setText(email_body, "UTF-8", "html");
			// message.set

			Transport.send(message);
			logger.info("Email Send SMS" + message);

		} catch (MessagingException e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));

			throw new RuntimeException(e);
		}
	}

}