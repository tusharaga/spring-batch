package com.cimm2.controller;

import org.apache.commons.collections.KeyValue;
import org.apache.commons.collections.keyvalue.DefaultKeyValue;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.ListableJobLocator;
import org.springframework.batch.core.launch.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Controller()
public class JobLaunchingController {


    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private ListableJobLocator jobLocator;

    @RequestMapping(method = RequestMethod.POST, headers = ("content-type=multipart/*"), produces = "application/json", consumes = MediaType.APPLICATION_FORM_URLENCODED_VALUE)
    @ResponseBody
    @ResponseStatus(value = HttpStatus.CREATED)
    public String launch(@RequestParam(value = "file", required = true) MultipartFile file) throws Exception {

        String fileName = file.getOriginalFilename();
        String fileExtension = FilenameUtils.getExtension(fileName);

        if (StringUtils.containsIgnoreCase("csv;xls;xlsx", fileExtension)) {
            Long jobId = this.jobOperator.start("MasterJob", "ImportJob" + Calendar.getInstance().getTimeInMillis());
            String response = "File uploaded successfully. Please use the job id '" + jobId + "' to query back the status of the import";
            return response;
        } else {
            return "Invalid file, supported files format are csv, xls and xlsx";
        }
    }

    @RequestMapping(value = "/job/start", method = RequestMethod.GET)
    @ResponseBody
    public String startNewJob(@RequestParam("jobName") String jobname,
                              @RequestParam("clientId") Long clientId) throws Exception {
        JobParameters jobParameters =
                new JobParametersBuilder()
                        .addLong("clientId", clientId).addString("invokedTime", new Date().toString())
                        .toJobParameters();
        Long jobId = this.jobOperator.start(jobname, jobParameters.toString());
        return "Started job, please use jobid=" + jobId + " to know job status";
    }

    @RequestMapping(value = "/job/list", method = RequestMethod.GET)
    @ResponseBody
    public Collection<String> jobList() throws Exception {
        Collection<String> jobNames = jobLocator.getJobNames();
        return jobNames;
    }

    @RequestMapping(value = "/job/start/{id}", method = RequestMethod.GET)
    @ResponseBody
    public String startOldJob(@PathVariable("id") Long id) throws Exception {
        Long jobId = this.jobOperator.restart(id);
        return "Restarted job, please use jobid=" + jobId + " to know job status";
    }

    @RequestMapping(value = "/job/{id}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void stop(@PathVariable("id") Long id) throws Exception {
        this.jobOperator.stop(id);
    }

    @RequestMapping(value = "/job/{id}", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    @ResponseBody
    public String get(@PathVariable("id") Long id) throws Exception {
        return this.jobOperator.getSummary(id);
    }

    // New API
    @RequestMapping(value = "/job/{jobName}/run", method = RequestMethod.POST)
    public @ResponseBody
    String start(@ModelAttribute JobParameters jobParameters, @PathVariable String jobName) {
        try {
            String string = jobOperator.start(jobName, jobParameters.toString()).toString();
            return "Started job, please use jobid=" + string + " to know job status";
        } catch (JobParametersInvalidException e) {
            return "JobParameters are invalid";
        } catch (NoSuchJobException e) {
            return "Job with name " + jobName + " not found";
        } catch (JobInstanceAlreadyExistsException e) {
            return "JobInstance for job " + jobName + " with parameters " + jobParameters + " already exists";
        }
    }

    @RequestMapping(value = "/job/{jobName}/status", method = RequestMethod.GET)
    public @ResponseBody
    String status(@PathVariable String jobName) {
        try {
            Set<Long> ids = jobOperator.getRunningExecutions(jobName);
            String status = "";
            for (Long id : ids) {
                try {
                    String text = jobOperator.getSummary(id);
                    Summary summary = Summary.parse(text);
                    status += summary.getAttribute("status").toString() + "<br/>";
                } catch (NoSuchJobExecutionException e) {
                    status += "Job with ID '" + id + "' is not running any more<br/>";
                }
            }
            return status;
        } catch (NoSuchJobException e) {
            return "Job with name '" + jobName + "' is not running";
        }
    }

    @RequestMapping(value = "/job/{jobName}/history", method = RequestMethod.GET)
    public @ResponseBody
    String history(@PathVariable String jobName) {
        try {
            List<Long> instanceIds = jobOperator.getJobInstances(jobName, 0, Integer.MAX_VALUE);
            String history = "";
            for (Long instanceId : instanceIds) {
                try {
                    List<Long> ids = jobOperator.getExecutions(instanceId);
                    for (Long id : ids) {
                        try {
                            String text = jobOperator.getSummary(id);
                            Summary summary = Summary.parse(text);
                            history += "Execution ID '" + id + "': " + summary.getAttribute("status").toString()
                                    + "<br/>";
                        } catch (NoSuchJobExecutionException e) {
                            history += "Job with ID '" + id + "' is not running any more<br/>";
                        }
                    }
                } catch (NoSuchJobInstanceException e1) {
                    return history += "Job instance with ID '" + instanceId + "' does not exist<br/>";
                }
            }
            return history;
        } catch (NoSuchJobException e) {
            return "Job with name '" + jobName + "' has not been started yet";
        }
    }

    private static class Summary {

        private final List<KeyValue> attributes = new ArrayList<>();

        public static Summary parse(String text) {
            Summary summary = new Summary();
            // see toString() method of JobExecution
            String regex = "(.*): id=(.*), version=(.*), startTime=(.*), endTime=(.*), lastUpdated=(.*), status=(.*), exitStatus=(.*), job=\\[(.*)\\], jobParameters=\\[(.*)\\]";
            Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
            Matcher matcher = pattern.matcher(text);
            if (matcher.find()) {
                summary.attributes.add(new DefaultKeyValue("className", matcher.group(1)));
                summary.attributes.add(new DefaultKeyValue("id", matcher.group(2)));
                summary.attributes.add(new DefaultKeyValue("version", matcher.group(3)));
                summary.attributes.add(new DefaultKeyValue("startTime", matcher.group(4)));
                summary.attributes.add(new DefaultKeyValue("endTime", matcher.group(5)));
                summary.attributes.add(new DefaultKeyValue("lastUpdated", matcher.group(6)));
                summary.attributes.add(new DefaultKeyValue("status", matcher.group(7)));
                summary.attributes.add(new DefaultKeyValue("exitStatus", matcher.group(8)));
                summary.attributes.add(new DefaultKeyValue("job", matcher.group(9)));
                summary.attributes.add(new DefaultKeyValue("jobParameters", matcher.group(10)));
            } else {
                throw new IllegalStateException("Pattern is not valid - see JobExecution toString() method");
            }
            return summary;
        }

        public Object getAttribute(Object key) {
            for (KeyValue attribute : attributes) {
                if (attribute.getKey().equals(key)) {
                    return attribute.getValue();
                }
            }
            throw new IllegalArgumentException("Key '" + key + "' was not found in attributes list");
        }
    }


}