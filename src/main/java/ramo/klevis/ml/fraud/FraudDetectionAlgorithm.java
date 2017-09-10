package ramo.klevis.ml.fraud;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Created by klevis.ramo on 9/5/2017.
 */
public class FraudDetectionAlgorithm implements Serializable {


    private final AlgorithmConfiguration algorithmConfiguration;

    public FraudDetectionAlgorithm(AlgorithmConfiguration algorithmConfiguration) throws Exception {
        this.algorithmConfiguration = algorithmConfiguration;
        setHadoopHomeEnvironmentVariable();
    }

    public void executeAlgorithm() throws IOException {
        JavaSparkContext sparkContext = createSparkContext();
        JavaRDD<LabeledPoint> labeledPointJavaRDD = loadDataFromFile(sparkContext);
        List<Integer> skipFeatures = algorithmConfiguration.getSkipFeatures();
        List<TransactionType> transactionTypesToExecute = algorithmConfiguration.getTransactionTypesToExecute();
        int run = 1;
        for (TransactionType transactionType : transactionTypesToExecute) {
            JavaRDD<LabeledPoint> filterRequestedByDataType = filterRequestedDataType(labeledPointJavaRDD, transactionType, skipFeatures, sparkContext);
            ResultsSummary resultsSummary = runAnomalyDetection(sparkContext, filterRequestedByDataType);
            resultsSummary.setId(run);
            resultsSummary.setTransactionType(transactionType);
        }

    }

    private ResultsSummary runAnomalyDetection(JavaSparkContext sc, JavaRDD<LabeledPoint> filteredDataByType) throws IOException {

        ResultsSummary resultsSummary = new ResultsSummary();
        JavaRDD<LabeledPoint> regularData = filteredDataByType.filter(e -> e.label() == (0d));
        JavaRDD<LabeledPoint> anomalies = filteredDataByType.filter(e -> e.label() == (1d));
        List<LabeledPoint> regularList = new ArrayList<>(regularData.collect());
        List<LabeledPoint> anomaliesList = new ArrayList<>(anomalies.collect());
        //randomize anomalies
        Collections.shuffle(anomaliesList);
        //randomize regular
        Collections.shuffle(regularList);

        resultsSummary.setTotalRegularSize(regularList.size());
        resultsSummary.setTotalFraudSize(anomaliesList.size());

        //choose 60% as train data with no anomalies
        int trainingDataSize = (int) (0.6 * regularList.size());
        List<LabeledPoint> trainData = generateTrainData(regularList, trainingDataSize);
        List<LabeledPoint> crossData = generateCrossData(regularList, anomaliesList, trainingDataSize, resultsSummary);
        List<LabeledPoint> testData = generateTestData(regularList, anomaliesList, trainingDataSize, resultsSummary);

        resultsSummary.setTrainDataSize(trainData.size());
        resultsSummary.setCrossTotalDataSize(crossData.size());
        resultsSummary.setTestTotalDataSize(testData.size());

        JavaRDD<LabeledPoint> paralleledTrainData = sc.parallelize(trainData);
        MultivariateStatisticalSummary summary = Statistics.colStats(paralleledTrainData.map(e -> e.features()).rdd());

        resultsSummary.setMean(summary.mean().toArray());
        resultsSummary.setSigma(summary.variance().toArray());

        Double bestEpsilon = findBestEpsilon(sc, crossData, summary);
        resultsSummary.setEpsilon(bestEpsilon);

        TestResult testResultFromTestData = testAlgorithmWithData(sc, testData, summary, bestEpsilon);
        resultsSummary.setTestFoundFraudSize(testResultFromTestData.foundFrauds);
        resultsSummary.setTestFlaggedAsFraud(testResultFromTestData.flaggedFrauds);
        resultsSummary.setTestNotFoundFraudSize(testResultFromTestData.missedFrauds);
        resultsSummary.setTestFraudSize(testResultFromTestData.totalFrauds);

        TestResult testResultFromCrossData = testAlgorithmWithData(sc, crossData, summary, bestEpsilon);
        resultsSummary.setCrossFoundFraudSize(testResultFromCrossData.foundFrauds);
        resultsSummary.setCrossFlaggedAsFraud(testResultFromCrossData.flaggedFrauds);
        resultsSummary.setCrossNotFoundFraudSize(testResultFromCrossData.missedFrauds);
        resultsSummary.setCrossFraudSize(testResultFromCrossData.totalFrauds);
        return resultsSummary;
    }

    private JavaSparkContext createSparkContext() {
        SparkConf conf = new SparkConf().setAppName("Finance Fraud Detection").setMaster("local[*]");
        return new JavaSparkContext(conf);
    }

    private TestResult testAlgorithmWithData(JavaSparkContext sc, List<LabeledPoint> testData, MultivariateStatisticalSummary summary, Double bestEpsilon) {
        JavaRDD<LabeledPoint> paralleledTestData = sc.parallelize(testData);
        MultivariateGaussian multivariateGaussian = new MultivariateGaussian(summary.mean(), DenseMatrix.diag(summary.variance()));
        JavaRDD<Tuple<Double, Double>> testDataProbabilityDenseFunction = paralleledTestData.map(e -> new Tuple<>(e.label(), multivariateGaussian.logpdf(e.features()))).cache();
        long totalFrauds = testDataProbabilityDenseFunction.filter(e -> e.label.equals(Double.valueOf(1))).count();
        long foundFrauds = testDataProbabilityDenseFunction.filter(e -> e.value < bestEpsilon
                && e.label.equals(Double.valueOf(1))).count();

        long flaggedFrauds = testDataProbabilityDenseFunction.filter(e -> e.value < bestEpsilon).count();

        long missedFrauds = testDataProbabilityDenseFunction.filter(e -> e.value > bestEpsilon
                && e.label.equals(Double.valueOf(1))).count();

        return new TestResult(totalFrauds, foundFrauds, flaggedFrauds, missedFrauds);
    }

    private Double findBestEpsilon(JavaSparkContext sc, List<LabeledPoint> crossData, MultivariateStatisticalSummary summary) {
        JavaRDD<LabeledPoint> paralleledCrossData = sc.parallelize(crossData);
        MultivariateGaussian multivariateGaussian = new MultivariateGaussian(summary.mean(), DenseMatrix.diag(summary.variance()));
        JavaRDD<Tuple<Double, Double>> trainDataProbabilityDenseFunction = paralleledCrossData.map(e -> new Tuple<>(e.label(), multivariateGaussian.logpdf(e.features())));
        Double min = trainDataProbabilityDenseFunction.min(new MinComparator()).value;
        Double max = trainDataProbabilityDenseFunction.max(new MaxComparator()).value;
        Double step = (max - min) / 1000d;
        List<Double> epsilons = new ArrayList<>();
        for (double epsilon = min; epsilon < max; epsilon = epsilon + step) {
            epsilons.add(epsilon);
        }
        List<Tuple<Double, Double>> trainDataProbabilityDenseFunctionList = trainDataProbabilityDenseFunction.collect();
        Double bestEpsilon = sc.parallelize(epsilons).reduce((e1, e2) -> {
                    double f1 = getF1(trainDataProbabilityDenseFunctionList, e1);
                    double f2 = getF1(trainDataProbabilityDenseFunctionList, e2);
                    if (f1 > f2) {
                        return e1;
                    } else {
                        return e2;
                    }
                }
        );
        return bestEpsilon;

    }

    private double getF1(List<Tuple<Double, Double>> trainDataProbabilityDenseFunctionList, Double epsilon) {
        long successfullyDetectedFrauds = trainDataProbabilityDenseFunctionList.stream()
                .filter(e -> e.value <= epsilon
                        && e.label.equals(Double.valueOf(1))).count();

        long wronglyFlaggedAsFrauds = trainDataProbabilityDenseFunctionList.stream()
                .filter(e -> e.value <= epsilon
                        && e.label.equals(Double.valueOf(0))).count();

        long missedFrauds = trainDataProbabilityDenseFunctionList.stream()
                .filter(e -> e.value > epsilon
                        && e.label.equals(Double.valueOf(1))).count();
        double prec = (double) successfullyDetectedFrauds / (double) (successfullyDetectedFrauds + wronglyFlaggedAsFrauds);

        double rec = (double) successfullyDetectedFrauds / (double) (successfullyDetectedFrauds + missedFrauds);
        return 2 * (prec * rec) / (prec + rec);
    }

    private List<LabeledPoint> generateTrainData(List<LabeledPoint> regularData, int trainingDataSize) {
        return regularData.stream().parallel().limit(trainingDataSize).collect(toList());
    }

    private List<LabeledPoint> generateTestData(List<LabeledPoint> regularData, List<LabeledPoint> anomalies, int trainingDataSize, ResultsSummary resultsSummary) {
        int crossRegularDataSize = (int) ((regularData.size() - trainingDataSize) * 0.5);
        //choose the rest as testAlgorithmWithData validation data with no anomalies
        List<LabeledPoint> testDataRegular = regularData.stream().parallel()
                .skip(trainingDataSize + crossRegularDataSize)
                .limit(regularData.size() - trainingDataSize + crossRegularDataSize).collect(toList());
        List<LabeledPoint> testAnomalies = anomalies.stream().skip(anomalies.size() / 2).limit(anomalies.size() - (anomalies.size() / 2)).collect(toList());
        List<LabeledPoint> testData = new ArrayList<>();
        testData.addAll(testDataRegular);
        testData.addAll(testAnomalies);
        resultsSummary.setTestRegularSize(testDataRegular.size());
        resultsSummary.setTestFraudSize(testAnomalies.size());
        return testData;
    }

    private ArrayList<LabeledPoint> generateCrossData(List<LabeledPoint> regularData, List<LabeledPoint> anomalies, int trainingDataSize, ResultsSummary resultsSummary) {
        //choose 20% as cross validation data with no anomalies
        int crossRegularDataSize = (int) ((regularData.size() - trainingDataSize) * 0.5);
        List<LabeledPoint> crossDataRegular = regularData.stream().parallel().skip(trainingDataSize).limit(crossRegularDataSize).collect(toList());
        List<LabeledPoint> crossDataAnomalies = anomalies.stream().limit(anomalies.size() / 2).collect(toList());
        ArrayList<LabeledPoint> crossData = new ArrayList<>();
        crossData.addAll(crossDataRegular);
        crossData.addAll(crossDataAnomalies);
        resultsSummary.setCrossRegularSize(crossDataRegular.size());
        resultsSummary.setCrossFraudSize(crossDataAnomalies.size());
        return crossData;
    }


    private JavaRDD<LabeledPoint> filterRequestedDataType(JavaRDD<LabeledPoint> data, TransactionType type, List<Integer> skipFeatures, JavaSparkContext sc) throws IOException {
        if (type == TransactionType.ALL) {
            return data.filter(e -> e != null).map(e -> skipSelectedFeatures(e, skipFeatures));
        } else {
            return data.filter(e -> e != null && e.features().apply(1) == type.getTransactionType()).map(e -> skipSelectedFeatures(e, skipFeatures));
        }
    }

    private JavaRDD<LabeledPoint> loadDataFromFile(JavaSparkContext sc) throws IOException {
        File file = new File(algorithmConfiguration.getFileName());
        return sc.textFile(file.getPath()).
                map(line -> {
                    double[] featureValues = Stream.of(line.split(",")).mapToDouble(e -> Double.parseDouble(e)).toArray();
                    if (algorithmConfiguration.isMakeFeaturesMoreGaussian()) {
                        makeFeaturesMoreGaussian(featureValues);
                    }
                    //always skip 9 and 10 because they are labels fraud or not fraud
                    featureValues = Arrays.copyOfRange(featureValues, 0, 9);
                    return new LabeledPoint(featureValues[9], Vectors.dense(featureValues));
                }).cache();
    }

    private void makeFeaturesMoreGaussian(double[] featureValues) {
        double[] powers = {0.5, 1, 0.1, 0.3, 0.1, 0.08, 0.3, 0.1, 0.1, 1, 1};
        for (int i = 0; i < featureValues.length; i++) {
            featureValues[i] = Math.pow(featureValues[i], powers[i]);
        }
    }

    private LabeledPoint skipSelectedFeatures(LabeledPoint labeledPoint, List<Integer> skipFeatures) {
        double[] featureValues = labeledPoint.features().toArray();
        double[] finalFeatureValues = new double[featureValues.length - skipFeatures.size()];
        int index = 0;
        for (int i = 0; i < featureValues.length; i++) {
            if (!skipFeatures.contains(i)) {
                finalFeatureValues[index] = featureValues[i];
            }
        }
        return new LabeledPoint(labeledPoint.label(), Vectors.dense(finalFeatureValues));
    }

    private class Tuple<F extends Serializable, S extends Serializable> implements Serializable {
        private F label;
        private S value;

        public Tuple(F label, S value) {

            this.label = label;
            this.value = value;
        }
    }

    private class MaxComparator implements Comparator<Tuple<Double, Double>>, Serializable {
        @Override
        public int compare(Tuple<Double, Double> o1, Tuple<Double, Double> o2) {
            return o1.value.compareTo(o2.value);
        }
    }

    private class MinComparator implements Comparator<Tuple<Double, Double>>, Serializable {
        @Override
        public int compare(Tuple<Double, Double> o1, Tuple<Double, Double> o2) {
            return o1.value.compareTo(o2.value);
        }
    }

    private void setHadoopHomeEnvironmentVariable() throws Exception {
        HashMap<String, String> hadoopEnvSetUp = new HashMap<>();
        hadoopEnvSetUp.put("HADOOP_HOME", new File(algorithmConfiguration.getHadoopApplicationPath()).getAbsolutePath());
        Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
        Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
        theEnvironmentField.setAccessible(true);
        Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
        env.clear();
        env.putAll(hadoopEnvSetUp);
        Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
        theCaseInsensitiveEnvironmentField.setAccessible(true);
        Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
        cienv.clear();
        cienv.putAll(hadoopEnvSetUp);
    }

    private class TestResult {
        private final long totalFrauds;
        private final long foundFrauds;
        private final long flaggedFrauds;
        private final long missedFrauds;

        public TestResult(long totalFrauds, long foundFrauds, long flaggedFrauds, long missedFrauds) {
            this.totalFrauds = totalFrauds;
            this.foundFrauds = foundFrauds;
            this.flaggedFrauds = flaggedFrauds;
            this.missedFrauds = missedFrauds;
        }
    }
}
