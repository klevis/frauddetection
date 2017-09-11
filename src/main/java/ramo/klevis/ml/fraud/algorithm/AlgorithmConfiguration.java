package ramo.klevis.ml.fraud.algorithm;

import org.apache.commons.lang3.StringUtils;
import ramo.klevis.ml.fraud.data.TransactionType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class AlgorithmConfiguration implements Serializable {

    private List<Integer> skipFeatures = new ArrayList<>();

    private List<TransactionType> transactionTypesToExecute = new ArrayList<>();

    private String fileName;

    private String hadoopApplicationPath;

    private boolean makeFeaturesMoreGaussian;
    private int runsTime;
    private String runsWith;
    private int trainDataNormalPercentage;
    private int trainDataFraudPercentage;
    private int testDataFraudPercentage;
    private int testDataNormalPercentage;
    private int crossDataFraudPercentage;
    private int crossDataNormalPercentage;

    private AlgorithmConfiguration(List<Integer> skipFeatures, List<TransactionType> transactionTypesToExecute, String fileName, String hadoopApplicationPath, boolean makeFeaturesMoreGaussian, int runsTime, String runsWith, int trainDataNormalPercentage, int trainDataFraudPercentage, int testDataFraudPercentage, int testDataNormalPercentage, int crossDataFraudPercentage, int crossDataNormalPercentage) {
        this.skipFeatures = skipFeatures;
        this.transactionTypesToExecute = transactionTypesToExecute;
        this.fileName = fileName;
        this.hadoopApplicationPath = hadoopApplicationPath;
        this.makeFeaturesMoreGaussian = makeFeaturesMoreGaussian;
        this.runsTime = runsTime;
        this.runsWith = runsWith;
        this.trainDataNormalPercentage = trainDataNormalPercentage;
        this.trainDataFraudPercentage = trainDataFraudPercentage;
        this.testDataFraudPercentage = testDataFraudPercentage;
        this.testDataNormalPercentage = testDataNormalPercentage;
        this.crossDataFraudPercentage = crossDataFraudPercentage;
        this.crossDataNormalPercentage = crossDataNormalPercentage;
    }

    public double getTrainDataNormalPercentage() {
        return trainDataNormalPercentage / 100d;
    }


    public double getTrainDataFraudPercentage() {
        return trainDataFraudPercentage / 100d;
    }


    public double getTestDataFraudPercentage() {
        return testDataFraudPercentage / 100d;
    }

    public double getTestDataNormalPercentage() {
        return testDataNormalPercentage / 100d;
    }


    public double getCrossDataFraudPercentage() {
        return crossDataFraudPercentage / 100d;
    }

    public double getCrossDataNormalPercentage() {
        return crossDataNormalPercentage / 100d;
    }

    public int getRunsTime() {
        return runsTime;
    }

    public String getRunsWith() {
        return runsWith;
    }

    public boolean isMakeFeaturesMoreGaussian() {
        return makeFeaturesMoreGaussian;
    }

    public List<Integer> getSkipFeatures() {
        return new ArrayList<>(skipFeatures);
    }

    public List<TransactionType> getTransactionTypesToExecute() {
        return new ArrayList<>(transactionTypesToExecute);
    }

    public String getFileName() {
        return fileName;
    }

    public String getHadoopApplicationPath() {
        return hadoopApplicationPath;
    }

    public static class AlgorithmConfigurationBuilder {
        private List<String> skipFeatures = new ArrayList<>();
        private List<String> transactionTypesToExecute = new ArrayList<>();

        private String fileName = "data/allData.csv";
        private String hadoopApplicationPath;
        private boolean makeFeaturesMoreGaussian = true;
        private int runsTime = 1;
        private String runsWith;
        private int trainDataNormalPercentage;
        private int trainDataFraudPercentage;
        private int testDataFraudPercentage;
        private int testDataNormalPercentage;
        private int crossDataFraudPercentage;
        private int crossDataNormalPercentage;

        public AlgorithmConfigurationBuilder withSkipFeatures(String... skipFeatures) {
            this.skipFeatures.addAll(Arrays.asList(skipFeatures));
            return this;
        }

        public AlgorithmConfigurationBuilder withTransactionTypes(String... transactionTypes) {
            transactionTypesToExecute.addAll(Arrays.asList(transactionTypes));
            return this;
        }

        public AlgorithmConfigurationBuilder withFileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public AlgorithmConfigurationBuilder withHadoopApplicationPath(String hadoopApplicationPath) {
            this.hadoopApplicationPath = hadoopApplicationPath;
            return this;
        }

        public AlgorithmConfigurationBuilder withMakeFeaturesMoreGaussian(boolean makeFeaturesMoreGaussian) {
            this.makeFeaturesMoreGaussian = makeFeaturesMoreGaussian;
            return this;
        }

        public AlgorithmConfigurationBuilder withRunsTime(int runsTime) {
            this.runsTime = runsTime;
            return this;
        }

        public AlgorithmConfigurationBuilder withRunsWith(String runsWith) {
            this.runsWith = runsWith;
            return this;
        }

        public AlgorithmConfigurationBuilder withTrainDataNormalPercentage(int trainDataNormalPercentage) {
            this.trainDataNormalPercentage = trainDataNormalPercentage;
            return this;
        }

        public AlgorithmConfigurationBuilder withTrainDataFraudPercentage(int trainDataFraudPercentage) {
            this.trainDataFraudPercentage = trainDataFraudPercentage;
            return this;
        }

        public AlgorithmConfigurationBuilder withTestDataFraudPercentage(int testDataFraudPercentage) {
            this.testDataFraudPercentage = testDataFraudPercentage;
            return this;
        }

        public AlgorithmConfigurationBuilder withTestDataNormalPercentage(int testDataNormalPercentage) {
            this.testDataNormalPercentage = testDataNormalPercentage;
            return this;
        }

        public AlgorithmConfigurationBuilder withCrossDataFraudPercentage(int crossDataFraudPercentage) {
            this.crossDataFraudPercentage = crossDataFraudPercentage;
            return this;
        }

        public AlgorithmConfigurationBuilder withCrossDataNormalPercentage(int crossDataNormalPercentage) {
            this.crossDataNormalPercentage = crossDataNormalPercentage;
            return this;
        }

        public AlgorithmConfiguration createAlgorithmConfiguration() {
            return new AlgorithmConfiguration(skipFeatures.stream().filter(e -> !StringUtils.isEmpty(e))
                    .map(e -> Integer.valueOf(e)).collect(Collectors.toList()),
                    transactionTypesToExecute.stream()
                            .map(e -> TransactionType.valueOf(e)).collect(Collectors.toList()),
                    fileName,
                    hadoopApplicationPath,
                    makeFeaturesMoreGaussian,
                    runsTime,
                    runsWith,
                    trainDataNormalPercentage,
                    trainDataFraudPercentage,
                    testDataFraudPercentage,
                    testDataNormalPercentage,
                    crossDataFraudPercentage,
                    crossDataNormalPercentage);
        }

    }

    @Override
    public String toString() {
        return "AlgorithmConfiguration{" + "\n" +
                "skipFeatures=" + skipFeatures + "\n" +
                ", transactionTypesToExecute=" + transactionTypesToExecute + "\n" +
                ", fileName='" + fileName + '\'' + "\n" +
                ", hadoopApplicationPath='" + hadoopApplicationPath + '\'' + "\n" +
                ", makeFeaturesMoreGaussian=" + makeFeaturesMoreGaussian +
                ", runsTime=" + runsTime + "\n" +
                ", runsWith='" + runsWith + '\'' + "\n" +
                ", trainDataNormalPercentage=" + trainDataNormalPercentage + "\n" +
                ", trainDataFraudPercentage=" + trainDataFraudPercentage + "\n" +
                ", testDataFraudPercentage=" + testDataFraudPercentage + "\n" +
                ", testDataNormalPercentage=" + testDataNormalPercentage + "\n" +
                ", crossDataFraudPercentage=" + crossDataFraudPercentage + "\n" +
                ", crossDataNormalPercentage=" + crossDataNormalPercentage + "\n" +
                '}';
    }
}
