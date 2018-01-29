package listener;

import com.cimm2.util.CommonUtil;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;

import java.util.Date;

public class StepListener {


    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        System.out.println(">> Before StepExecution Listener Thread:" + Thread.currentThread().getName() + ", Object hashcode:" + this);
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        System.out.println(">> After StepExecution Listener Thread:" + Thread.currentThread().getName() + ", Object hashcode:" + this);
        Date startTime = stepExecution.getStartTime();
        Date endTime = new Date();
        String stepName = stepExecution.getStepName();

        System.out.println("--Step-- " + stepName + " took " + CommonUtil.getTimeDiff(endTime, startTime));
        return ExitStatus.COMPLETED;
    }

}