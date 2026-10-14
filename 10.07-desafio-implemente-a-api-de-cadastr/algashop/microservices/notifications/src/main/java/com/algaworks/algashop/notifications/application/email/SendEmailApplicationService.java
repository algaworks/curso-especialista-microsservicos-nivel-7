package com.algaworks.algashop.notifications.application.email;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class SendEmailApplicationService {

	private final EmailTemplateRenderer renderer;
	private final EmailSender emailSender;

	public void send(SendEmailInput input) {

	}

}
