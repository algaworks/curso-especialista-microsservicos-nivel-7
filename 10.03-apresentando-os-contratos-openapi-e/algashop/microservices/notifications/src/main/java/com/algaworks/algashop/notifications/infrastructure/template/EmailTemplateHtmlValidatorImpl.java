package com.algaworks.algashop.notifications.infrastructure.template;

import com.algaworks.algashop.notifications.application.email.EmailTemplateHtmlValidator;

import org.springframework.stereotype.Component;

@Component
public class EmailTemplateHtmlValidatorImpl implements EmailTemplateHtmlValidator {


	@Override
	public void validateSyntax(String html) {

	}
}
