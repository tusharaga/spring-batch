package com.cimm2;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class Cimm2ExportApplication {

    public static void main(String[] args) {
        SpringApplication.run(Cimm2ExportApplication.class, args);
    }
}
