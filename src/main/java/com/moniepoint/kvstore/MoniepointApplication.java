package com.moniepoint.kvstore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MoniepointApplication {

	public static void main(String[] args) {
		SpringApplication.run(MoniepointApplication.class, args);
	}

}
