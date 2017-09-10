package ramo.klevis.ml.fraud;

/**
 * Created by klevis.ramo on 9/10/2017.
 */
public class ResultsSummary {

    private long regularSize;
    private long fraudSize;

    private long trainDataSize;
    private long crossDataSize;
    private long testDataSize;

    private long timeInMilliseconds;

    private double epsilon;
    private double mu;
    private double sigma;

    private long notFoundFraudSize;
    private long foundFraudSize;
    private long flaggedAsFraud;

    private double successPercentage;
    private double failPercentage;

    public long getRegularSize() {
        return regularSize;
    }

    public void setRegularSize(long regularSize) {
        this.regularSize = regularSize;
    }

    public long getFraudSize() {
        return fraudSize;
    }

    public void setFraudSize(long fraudSize) {
        this.fraudSize = fraudSize;
    }

    public long getTrainDataSize() {
        return trainDataSize;
    }

    public void setTrainDataSize(long trainDataSize) {
        this.trainDataSize = trainDataSize;
    }

    public long getCrossDataSize() {
        return crossDataSize;
    }

    public void setCrossDataSize(long crossDataSize) {
        this.crossDataSize = crossDataSize;
    }

    public long getTestDataSize() {
        return testDataSize;
    }

    public void setTestDataSize(long testDataSize) {
        this.testDataSize = testDataSize;
    }

    public long getTimeInMilliseconds() {
        return timeInMilliseconds;
    }

    public void setTimeInMilliseconds(long timeInMilliseconds) {
        this.timeInMilliseconds = timeInMilliseconds;
    }

    public double getEpsilon() {
        return epsilon;
    }

    public void setEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }

    public double getMu() {
        return mu;
    }

    public void setMu(double mu) {
        this.mu = mu;
    }

    public double getSigma() {
        return sigma;
    }

    public void setSigma(double sigma) {
        this.sigma = sigma;
    }

    public long getNotFoundFraudSize() {
        return notFoundFraudSize;
    }

    public void setNotFoundFraudSize(long notFoundFraudSize) {
        this.notFoundFraudSize = notFoundFraudSize;
    }

    public long getFoundFraudSize() {
        return foundFraudSize;
    }

    public void setFoundFraudSize(long foundFraudSize) {
        this.foundFraudSize = foundFraudSize;
    }

    public long getFlaggedAsFraud() {
        return flaggedAsFraud;
    }

    public void setFlaggedAsFraud(long flaggedAsFraud) {
        this.flaggedAsFraud = flaggedAsFraud;
    }

    public double getSuccessPercentage() {
        return successPercentage;
    }

    public void setSuccessPercentage(double successPercentage) {
        this.successPercentage = successPercentage;
    }

    public double getFailPercentage() {
        return failPercentage;
    }

    public void setFailPercentage(double failPercentage) {
        this.failPercentage = failPercentage;
    }
}
