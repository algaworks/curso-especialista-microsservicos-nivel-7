package com.algaworks.algashop.notifications.application.template.management;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class EmailTemplateManagementApplicationService {

	public UUID create(EmailTemplateInput input) {
		return null;
	}
	public void update(UUID id, EmailTemplateInput input) {

	}

	public void delete(UUID id) {
	}

}
