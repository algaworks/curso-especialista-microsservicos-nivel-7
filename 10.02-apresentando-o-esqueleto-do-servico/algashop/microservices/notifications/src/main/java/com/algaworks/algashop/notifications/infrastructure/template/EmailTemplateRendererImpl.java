package com.algaworks.algashop.notifications.infrastructure.template;

import com.algaworks.algashop.notifications.application.email.EmailTemplateRenderer;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class EmailTemplateRendererImpl implements EmailTemplateRenderer {

	@Override
	public String renderSubject(String subjectTemplate, Map<String, Object> variables) {
		return "";
	}

	@Override
	public String renderBody(String bodyTemplate, Map<String, Object> variables) {
		return "";
	}

	@Override
	public void assertRenderable(String subjectTemplate, String bodyTemplate) {

	}
}
