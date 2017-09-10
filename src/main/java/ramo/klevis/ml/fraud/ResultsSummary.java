package ramo.klevis.ml.fraud;

import java.util.Arrays;

/**
 * Created by klevis.ramo on 9/10/2017.
 */
public class ResultsSummary {

    private long trainDataSize;

    private long timeInMilliseconds;

    private double epsilon;
    private double[] mu;
    private double[] sigma;

    private long testNotFoundFraudSize;
    private long testFoundFraudSize;
    private long testFlaggedAsFraud;
    private long testFraudSize;
    private long testRegularSize;
    private long testTotalDataSize;

    private double successPercentage;
    private double failPercentage;

    private long totalRegularSize;
    private long totalFraudSize;
    private long totalNotFoundFraudSize;
    private long totalFoundFraudSize;
    private long totalFlaggedAsFraud;

    private long crossFoundFraudSize;
    private long crossFlaggedAsFraud;
    private long crossNotFoundFraudSize;
    private long crossFraudSize;
    private long crossRegularSize;
    private long crossTotalDataSize;
    private int id;
    private TransactionType transactionType;
    private AlgorithmConfiguration algorithmConfiguration;

    public ResultsSummary() {

    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }

    public long getCrossRegularSize() {
        return crossRegularSize;
    }

    public void setCrossRegularSize(long crossRegularSize) {
        this.crossRegularSize = crossRegularSize;
    }

    public long getTestTotalDataSize() {
        return testTotalDataSize;
    }

    public void setTestTotalDataSize(long testTotalDataSize) {
        this.testTotalDataSize = testTotalDataSize;
    }

    public long getTestRegularSize() {
        return testRegularSize;
    }

    public void setTestRegularSize(long testRegularSize) {
        this.testRegularSize = testRegularSize;
    }

    public long getTotalNotFoundFraudSize() {
        return totalNotFoundFraudSize;
    }

    public long getTotalFoundFraudSize() {
        return totalFoundFraudSize;
    }

    public long getTotalFlaggedAsFraud() {
        return totalFlaggedAsFraud;
    }

    public long getTotalRegularSize() {
        return totalRegularSize;
    }

    public void setTotalRegularSize(long totalRegularSize) {
        this.totalRegularSize = totalRegularSize;
    }

    public long getTotalFraudSize() {
        return totalFraudSize;
    }

    public void setTotalFraudSize(long totalFraudSize) {
        this.totalFraudSize = totalFraudSize;
    }

    public long getTrainDataSize() {
        return trainDataSize;
    }

    public void setTrainDataSize(long trainDataSize) {
        this.trainDataSize = trainDataSize;
    }

    public long getCrossTotalDataSize() {
        return crossTotalDataSize;
    }

    public void setCrossTotalDataSize(long crossTotalDataSize) {
        this.crossTotalDataSize = crossTotalDataSize;
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


    public double getFailPercentage() {
        return failPercentage;
    }

    public long getTestFraudSize() {
        return testFraudSize;
    }

    public void setTestFraudSize(long testFraudSize) {
        this.testFraudSize = testFraudSize;
    }

    public long getCrossFoundFraudSize() {
        return crossFoundFraudSize;
    }

    public void setCrossFoundFraudSize(long crossFoundFraudSize) {
        this.crossFoundFraudSize = crossFoundFraudSize;
    }

    public long getCrossFlaggedAsFraud() {
        return crossFlaggedAsFraud;
    }

    public void setCrossFlaggedAsFraud(long crossFlaggedAsFraud) {
        this.crossFlaggedAsFraud = crossFlaggedAsFraud;
    }

    public long getCrossNotFoundFraudSize() {
        return crossNotFoundFraudSize;
    }

    public void setCrossNotFoundFraudSize(long crossNotFoundFraudSize) {
        this.crossNotFoundFraudSize = crossNotFoundFraudSize;
    }

    public long getCrossFraudSize() {
        return crossFraudSize;
    }

    public void setCrossFraudSize(long crossFraudSize) {
        this.crossFraudSize = crossFraudSize;
    }

    public void setAlgorithmConfiguration(AlgorithmConfiguration algorithmConfiguration) {
        this.algorithmConfiguration = algorithmConfiguration;
    }

    public AlgorithmConfiguration getAlgorithmConfiguration() {
        return algorithmConfiguration;
    }

    @Override
    public String toString() {
        return "ResultsSummary{" +
                "trainDataSize=" + trainDataSize +
                ", timeInMilliseconds=" + timeInMilliseconds +
                ", epsilon=" + epsilon +
                ", mu=" + Arrays.toString(mu) +
                ", sigma=" + Arrays.toString(sigma) +
                ", testNotFoundFraudSize=" + testNotFoundFraudSize +
                ", testFoundFraudSize=" + testFoundFraudSize +
                ", testFlaggedAsFraud=" + testFlaggedAsFraud +
                ", testFraudSize=" + testFraudSize +
                ", testRegularSize=" + testRegularSize +
                ", testTotalDataSize=" + testTotalDataSize +
                ", successPercentage=" + successPercentage +
                ", failPercentage=" + failPercentage +
                ", totalRegularSize=" + totalRegularSize +
                ", totalFraudSize=" + totalFraudSize +
                ", totalNotFoundFraudSize=" + totalNotFoundFraudSize +
                ", totalFoundFraudSize=" + totalFoundFraudSize +
                ", totalFlaggedAsFraud=" + totalFlaggedAsFraud +
                ", crossFoundFraudSize=" + crossFoundFraudSize +
                ", crossFlaggedAsFraud=" + crossFlaggedAsFraud +
                ", crossNotFoundFraudSize=" + crossNotFoundFraudSize +
                ", crossFraudSize=" + crossFraudSize +
                ", crossRegularSize=" + crossRegularSize +
                ", crossTotalDataSize=" + crossTotalDataSize +
                ", id=" + id +
                ", transactionType=" + transactionType +
                ", algorithmConfiguration=" + algorithmConfiguration +
                '}';
    }
}
