package ramo.klevis.ml.fraud;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class AlgorithmConfiguration implements Serializable {

    private List<Integer> skipFeatures = new ArrayList<>();

    private List<TransactionType> transactionTypesToExecute = new ArrayList<>();

    private String fileName;

    private String hadoopApplicationPath;

    private boolean makeFeaturesMoreGaussian;
    private int runsTime;
    private String runsWith;

    private AlgorithmConfiguration(List<Integer> skipFeatures, List<TransactionType> transactionTypesToExecute, String fileName, String hadoopApplicationPath, boolean makeFeaturesMoreGaussian, int runsTime, String runsWith) {
        this.skipFeatures = skipFeatures;
        this.transactionTypesToExecute = transactionTypesToExecute;
        this.fileName = fileName;
        this.hadoopApplicationPath = hadoopApplicationPath;
        this.makeFeaturesMoreGaussian = makeFeaturesMoreGaussian;
        this.runsTime = runsTime;
        this.runsWith = runsWith;
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

        public AlgorithmConfiguration createAlgorithmConfiguration() {
            return new AlgorithmConfiguration(skipFeatures.stream()
                    .map(e -> Integer.valueOf(e)).collect(Collectors.toList()),
                    transactionTypesToExecute.stream()
                            .map(e -> TransactionType.valueOf(e)).collect(Collectors.toList()),
                    fileName,
                    hadoopApplicationPath,
                    makeFeaturesMoreGaussian,
                    runsTime,
                    runsWith);
        }

    }
}
