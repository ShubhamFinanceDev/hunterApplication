package com.hunter.hunter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

@Component
@Endpoint(id="customActuator")
public class CustomActuator {

	@Value("${management.server.port}") 
	private String acuatorPort;
	@ReadOperation
	public String currentDbDetails()
	{
		return "Give Current DB Status "+acuatorPort;
	}
}
