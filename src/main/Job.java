package main;

import java.io.Serializable;

public class Job implements Serializable {
    private final String jobType;
    private final int id;
    private boolean isFinished = false;

    public Job(String jobType, int id) {
        this.jobType = jobType;
        this.id = id;
    }

    public String getJobType() {
        return jobType;
    }

    public int getId() {
        return id;
    }

    public void setIsFinished(boolean isFinished) {
        this.isFinished = isFinished;
    }

    public boolean getIsFinished() {
        return isFinished;
    }
}
