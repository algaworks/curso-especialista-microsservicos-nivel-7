package com.algaworks.algashop.notifications.infrastructure.mail;

import com.algaworks.algashop.notifications.application.email.EmailSender;
import org.springframework.stereotype.Service;

@Service
public class EmailSenderJavaMailImpl implements EmailSender {

	@Override
	public void send(String toEmail, String toName, String subject, String htmlBody) {

	}
}
