package com.cimm2.util;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public abstract class CommonUtil {

    public static String getTimeDiff(Date dateOne, Date dateTwo) {
        String diff = "";
        long timeDiff = Math.abs(dateOne.getTime() - dateTwo.getTime());
        diff = String.format("%d hour(s) %d min(s) %d sec(s)", TimeUnit.MILLISECONDS.toHours(timeDiff),
                TimeUnit.MILLISECONDS.toMinutes(timeDiff) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(timeDiff)),
                TimeUnit.MILLISECONDS.toSeconds(timeDiff) -
                        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(timeDiff)));
        return diff;
    }
}
