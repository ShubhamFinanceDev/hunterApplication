package com.hunter.hunter;

//File Name SendEmail.java

import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;
import javax.activation.*;

public class SendEmail {

	public static void main(String[] args) {
		final String username = "alerts.etl@shubham.co";
		final String password = "Vijay@12";

		Properties props = new Properties();
		
		props.put("mail.smtp.host", "smtp.shubham.co");
		props.put("mail.smtp.socketFactory.port", "465");
		props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.port", "465");
		//props.setProperty("mail.smtp.ssl.enable", "false");
		//props.setProperty("java.net.preferIPv4Stack", "true");

		Session session = Session.getInstance(props, new javax.mail.Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}
		});

		try {

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress("alerts.etl@shubham.co"));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse("vijay.uniyal@shubham.co"));
			message.setSubject("Test");
			message.setText("HI");

			Transport.send(message);

			System.out.println("Done");

		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}
}