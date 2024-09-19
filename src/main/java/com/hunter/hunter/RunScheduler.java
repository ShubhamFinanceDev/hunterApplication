package com.hunter.hunter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.mail.MessagingException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
@EnableScheduling
public class RunScheduler {

	@Autowired
	@Qualifier("jdbcTemplate2")
	private JdbcTemplate osourceTemplate;

	//@Scheduled(cron = "0 0/2 * * * ?")
	@Scheduled(cron = "0 0 9,16 * * ?")
	public void scheduleFixedDelayTask() throws MessagingException {

		try {
			URL url = new URL("http://localhost:8080/fetchData");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				System.out.println(inputLine);
			}

			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	
	//
	//@Scheduled(cron = "0 0/2 * * * ?")
	public void scheduleFixedDelayTaskadhocfetchData() throws MessagingException {

		try {
			URL url = new URL("http://localhost:8080/adhocfetchData");
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");

			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				System.out.println(inputLine);
			}

			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
