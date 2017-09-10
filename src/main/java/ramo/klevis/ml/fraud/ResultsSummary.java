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
    private double[] mu;
    private double[] sigma;

    private long testNotFoundFraudSize;
    private long testFoundFraudSize;
    private long testFlaggedAsFraud;
    private long testFraudSize;

    private double successPercentage;
    private double failPercentage;
    private long crossFoundFraudSize;
    private long crossFlaggedAsFraud;
    private long crossNotFoundFraudSize;
    private long crossFraudSize;

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

    public double[] getMu() {
        return mu;
    }

    public void setMean(double[] mu) {
        this.mu = mu;
    }

    public double[] getSigma() {
        return sigma;
    }

    public void setSigma(double[] sigma) {
        this.sigma = sigma;
    }

    public long getTestNotFoundFraudSize() {
        return testNotFoundFraudSize;
    }

    public void setTestNotFoundFraudSize(long notFoundFraudSize) {
        this.testNotFoundFraudSize = notFoundFraudSize;
    }

    public long getTestFoundFraudSize() {
        return testFoundFraudSize;
    }

    public void setTestFoundFraudSize(long foundFraudSize) {
        this.testFoundFraudSize = foundFraudSize;
    }

    public long getTestFlaggedAsFraud() {
        return testFlaggedAsFraud;
    }

    public void setTestFlaggedAsFraud(long flaggedAsFraud) {
        this.testFlaggedAsFraud = flaggedAsFraud;
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

    public void setTestFraudSize(long testFraudSize) {
        this.testFraudSize = testFraudSize;
    }

    public long getTestFraudSize() {
        return testFraudSize;
    }

    public void setCrossFoundFraudSize(long crossFoundFraudSize) {
        this.crossFoundFraudSize = crossFoundFraudSize;
    }

    public long getCrossFoundFraudSize() {
        return crossFoundFraudSize;
    }

    public void setCrossFlaggedAsFraud(long crossFlaggedAsFraud) {
        this.crossFlaggedAsFraud = crossFlaggedAsFraud;
    }

    public long getCrossFlaggedAsFraud() {
        return crossFlaggedAsFraud;
    }

    public void setCrossNotFoundFraudSize(long crossNotFoundFraudSize) {
        this.crossNotFoundFraudSize = crossNotFoundFraudSize;
    }

    public long getCrossNotFoundFraudSize() {
        return crossNotFoundFraudSize;
    }

    public void setCrossFraudSize(long crossFraudSize) {
        this.crossFraudSize = crossFraudSize;
    }

    public long getCrossFraudSize() {
        return crossFraudSize;
    }
}
