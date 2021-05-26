package main;

import java.io.Serializable;

public class Job implements Serializable {
    private final String jobType;
    private final String id;
    private boolean isFinished = false;

    public Job(String jobType, int id, Client client) {
        this.jobType = jobType;
        this.id = client.toString() + id;
    }

    public String getJobType() {
        return jobType;
    }

    public String getId() {
        return id;
    }

    public void setIsFinished(boolean isFinished) {
        this.isFinished = isFinished;
    }

    public boolean getIsFinished() {
        return isFinished;
    }

    public String toString(){
        return "JOB{" + getId() + ", " + getJobType() + "}";
    }
}
