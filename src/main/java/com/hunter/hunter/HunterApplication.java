/*
 * 


Spring Boot Interview Questions And Answers || Spring Boot Tricky Interview Questions [MOST ASKED]

Spring Boot makes it easy to create stand-alone, production-grade Spring based Applications that you can "just run". Spring Boot helps you accelerate application development.

 It looks at your classpath and at the beans you have configured, makes reasonable assumptions about what you are missing, and adds those items. With Spring Boot, you can focus more on business features and less on infrastructure.

For all Spring applications, you should start with the Spring Initializr. The Initializr offers a fast way to pull in all the dependencies you need for an application and does a lot of the setup for you.

Spring Boot Interview questions are usually tricky ones and we need to practice them before appearing for spring boot interviews.

A very common question asked is what is @SpringBootAnnotation

@SpringBootApplication is a convenience annotation that adds all of the following:

@Configuration: Tags the class as a source of bean definitions for the application context.

@EnableAutoConfiguration: Tells Spring Boot to start adding beans based on classpath settings, other beans, and various property settings. For example, if spring-web MVC is on the classpath, this annotation flags the application as a web application and activates key behaviors, such as setting up a DispatcherServlet.

@ComponentScan: Tells Spring to look for other components, configurations, and services in the com/example package, letting it find the controllers.

This video of spring boot interview questions and answers contains Top basic important spring boot interview questions and answers with a Live Demo.

 Spring Boot Interview Questions And Answer | Spring Boot Interview Question Part 2 [WITH LIVE DEMO]
  here contains a live demo and code base for multiple interview questions like :
Spring boot interview questions for experienced includes these imp questions
What is Spring Actuator? What are its advantages?

An actuator is a manufacturing term that refers to a mechanical device for moving or controlling something. Actuators can generate a large amount of motion from a small change.

In Spring boot whenever something goes wrong we need to debug and go through logs to see the issue.

Using Spring Actuator, you can access those flows like what bean is created, what is the CPU usage. And many more features.

By Default Exposed HTTP endpoints can be seen at 
http://localhost:8090/actuator/httptrace.

How to create custom Endpoints -?
This can be  achieved by adding the following annotations:
@Endpoint and @Component to class
@ReadOperation, @WriteOperation, or @DeleteOperation on method-level

@ReadOperation maps to HTTP GET
@WriteOperation maps to HTTP POST
@DeleteOperation maps to HTTP DELETE


This end point Displays HTTP trace information (by default, the last 100 HTTP request-response exchanges). Requires an HttpTraceRepository bean.

YAML and properties file difference and why to use YAML over properties file is also covered in this video.
 
 * */

package com.hunter.hunter;

import org.springframework.boot.*;
import org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
//import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication(exclude = {
	   // DataSourceAutoConfiguration.class, 
	    DataSourceTransactionManagerAutoConfiguration.class, 
	    //HibernateJpaAutoConfiguration.class,
	    SecurityAutoConfiguration.class,
	    ManagementWebSecurityAutoConfiguration.class
	})
@ComponentScan("com.hunter")
public class HunterApplication {

	public static void main(String[] args) {
		SpringApplication.run(HunterApplication.class, args);
	}

}
