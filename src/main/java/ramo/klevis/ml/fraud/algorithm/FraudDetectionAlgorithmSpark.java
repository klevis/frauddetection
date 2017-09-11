package ramo.klevis.ml.fraud.algorithm;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian;
import ramo.klevis.ml.fraud.data.GeneratedData;
import ramo.klevis.ml.fraud.data.ResultsSummary;
import ramo.klevis.ml.fraud.data.TestResult;
import ramo.klevis.ml.fraud.data.TransactionType;
import ramo.klevis.ml.fraud.data.Tuple;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by klevis.ramo on 9/5/2017.
 */
public class FraudDetectionAlgorithmSpark extends AlgorithmTemplateExecution<JavaRDD<LabeledPoint>> {

    public FraudDetectionAlgorithmSpark(AlgorithmConfiguration algorithmConfiguration) {
        super(algorithmConfiguration);
    }


    protected JavaRDD<LabeledPoint> filterAnomaliesData(JavaRDD<LabeledPoint> filteredDataByType) {
        JavaRDD<LabeledPoint> anomaliesRDD = filteredDataByType.filter(e -> e.label() == (1d));
        return anomaliesRDD;
    }

    protected JavaRDD<LabeledPoint> filterRegularData(JavaRDD<LabeledPoint> filteredDataByType) {
        JavaRDD<LabeledPoint> regularDataRDD = filteredDataByType.filter(e -> e.label() == (0d));
        return regularDataRDD;
    }


    protected TestResult testAlgorithmWithData(JavaSparkContext sc, JavaRDD<LabeledPoint> testData, MultivariateStatisticalSummary summary, Double bestEpsilon) {
        MultivariateGaussian multivariateGaussian = new MultivariateGaussian(summary.mean(), DenseMatrix.diag(summary.variance()));
        JavaRDD<Tuple<Double, Double>> testDataProbabilityDenseFunction = testData.map(e -> new Tuple<>(e.label(), multivariateGaussian.logpdf(e.features())));
        long totalFrauds = testDataProbabilityDenseFunction.filter(e -> e.getLabel().equals(Double.valueOf(1))).count();
        long foundFrauds = testDataProbabilityDenseFunction.filter(e -> e.getValue() < bestEpsilon
                && e.getLabel().equals(Double.valueOf(1))).count();

        long flaggedFrauds = testDataProbabilityDenseFunction.filter(e -> e.getValue() < bestEpsilon).count();

        long missedFrauds = testDataProbabilityDenseFunction.filter(e -> e.getValue() > bestEpsilon
                && e.getLabel().equals(Double.valueOf(1))).count();

        return new TestResult(totalFrauds, foundFrauds, flaggedFrauds, missedFrauds);
    }

    protected Double findBestEpsilon(JavaSparkContext sc, GeneratedData<JavaRDD<LabeledPoint>> generatedData, MultivariateStatisticalSummary summary) {
        MultivariateGaussian multivariateGaussian = new MultivariateGaussian(summary.mean(), DenseMatrix.diag(summary.variance()));
        JavaRDD<Tuple<Double, Double>> trainDataProbabilityDenseFunction = generatedData.regularAndAnomalyData.map(e -> new Tuple<>(e.label(), multivariateGaussian.logpdf(e.features())));
        Double min = trainDataProbabilityDenseFunction.min(new SerializableTupleComparator()).getValue();
        Double max = trainDataProbabilityDenseFunction.max(new SerializableTupleComparator()).getValue();
        Double step = (max - min) / 1000d;
        List<Double> epsilons = new ArrayList<>();
        for (double epsilon = min; epsilon < max; epsilon = epsilon + step) {
            epsilons.add(epsilon);
        }
        List<Tuple<Double, Double>> trainDataProbabilityDenseFunctionList = trainDataProbabilityDenseFunction.collect();
        return sc.parallelize(epsilons).reduce((e1, e2) -> {
                    double f11 = getF1(trainDataProbabilityDenseFunctionList, e1);
                    double f12 = getF1(trainDataProbabilityDenseFunctionList, e2);
                    if (f11 > f12) {
                        return e1;
                    } else {
                        return e2;
                    }
                }
        );
    }

    private double getF1(List<Tuple<Double, Double>> trainDataProbabilityDenseFunctionList, Double epsilon) {
        long successfullyDetectedFrauds = trainDataProbabilityDenseFunctionList.stream()
                .filter(e -> e.getValue() <= epsilon
                        && e.getLabel().equals(Double.valueOf(1))).count();

        long wronglyFlaggedAsFrauds = trainDataProbabilityDenseFunctionList.stream()
                .filter(e -> e.getValue() <= epsilon
                        && e.getLabel().equals(Double.valueOf(0))).count();

        long missedFrauds = trainDataProbabilityDenseFunctionList.stream()
                .filter(e -> e.getValue() > epsilon
                        && e.getLabel().equals(Double.valueOf(1))).count();
        double prec = (double) successfullyDetectedFrauds / (double) (successfullyDetectedFrauds + wronglyFlaggedAsFrauds);

        double rec = (double) successfullyDetectedFrauds / (double) (successfullyDetectedFrauds + missedFrauds);
        return 2 * (prec * rec) / (prec + rec);
    }

    protected GeneratedData randomlyGenerateData(int normalSize, int fraudSize, JavaRDD<LabeledPoint> regularData, JavaRDD<LabeledPoint> anomalies, JavaSparkContext sparkContext) {

        double wightNormal = normalSize / (double) regularData.count();
        JavaRDD<LabeledPoint>[] regularSplit = regularData.randomSplit(new double[]{wightNormal, 1 - wightNormal});

        double wightFraud = fraudSize / (double) anomalies.count();
        JavaRDD<LabeledPoint>[] fraudSplit = new JavaRDD[2];
        if (!Double.isNaN(wightFraud)) {
            fraudSplit = anomalies.randomSplit(new double[]{wightFraud, 1 - wightFraud});
        } else {
            fraudSplit[0] = sparkContext.emptyRDD();
            fraudSplit[1] = sparkContext.emptyRDD();
        }
        return new GeneratedData(regularSplit[0], fraudSplit[0], regularSplit[0].union(fraudSplit[0]), regularSplit[1], fraudSplit[1]);
    }


    protected JavaRDD<LabeledPoint> filterRequestedDataType(JavaRDD<LabeledPoint> data, List<TransactionType> types, List<Integer> skipFeatures, JavaSparkContext sc) throws IOException {

        return data.filter(e -> e != null &&
                types.stream().filter(type -> (type == TransactionType.ALL
                        || type.getTransactionType() == e.features().apply(1))).findAny().isPresent())
                .map(e -> skipSelectedFeatures(e, skipFeatures));

    }

    @Override
    protected JavaRDD<LabeledPoint> getTestData(GeneratedData<JavaRDD<LabeledPoint>> crossData) {
        return crossData.leftRegular.union(crossData.leftAnomalies);
    }

    @Override
    protected MultivariateStatisticalSummary getMultivariateSummary(GeneratedData<JavaRDD<LabeledPoint>> trainData) {
        return Statistics.colStats(trainData.regularAndAnomalyData.map(e -> e.features()).rdd());
    }

    @Override
    protected void setTestDataSizes(ResultsSummary resultsSummary, GeneratedData<JavaRDD<LabeledPoint>> crossData) {
        resultsSummary.setTestRegularSize(crossData.leftRegular.count());
        resultsSummary.setTestFraudSize(crossData.leftAnomalies.count());
        resultsSummary.setTestTotalDataSize(crossData.leftRegular.count() + crossData.leftAnomalies.count());
    }

    @Override
    protected void setCrossDataSizes(ResultsSummary resultsSummary, GeneratedData<JavaRDD<LabeledPoint>> crossData) {
        resultsSummary.setCrossRegularSize(crossData.regular.count());
        resultsSummary.setCrossFraudSize(crossData.anomaly.count());
        resultsSummary.setCrossTotalDataSize(crossData.regularAndAnomalyData.count());
    }

    @Override
    protected void setTrainDataSizes(ResultsSummary resultsSummary, GeneratedData<JavaRDD<LabeledPoint>> trainData) {
        resultsSummary.setTrainRegularSize(trainData.regular.count());
        resultsSummary.setTrainFraudSize(trainData.anomaly.count());
        resultsSummary.setTrainRegularSize(trainData.regularAndAnomalyData.count());
    }

    @Override
    protected long getTotalAnomalies(JavaRDD<LabeledPoint> anomalies) {
        return anomalies.count();
    }

    @Override
    protected long getTotalRegular(JavaRDD<LabeledPoint> regular) {
        return regular.count();
    }

    protected JavaRDD<LabeledPoint> loadDataFromFile(JavaSparkContext sc) throws IOException {
        File file = new File(algorithmConfiguration.getFileName());

        return sc.textFile(file.getPath()).
                map(line -> {
                    line = line.replace(TransactionType.PAYMENT.name(), "1")
                            .replace(TransactionType.TRANSFER.name(), "2")
                            .replace(TransactionType.CASH_OUT.name(), "3")
                            .replace(TransactionType.DEBIT.name(), "4")
                            .replace(TransactionType.CASH_IN.name(), "5")
                            .replace("C", "1")
                            .replace("M", "2");
                    String[] split = line.split(",");
                    //skip header
                    if (split[0].equalsIgnoreCase("step")) {
                        return null;
                    }
                    double[] featureValues = Stream.of(split)
                            .mapToDouble(e -> Double.parseDouble(
                                    e
                                    .replaceAll(TransactionType.PAYMENT.name(), "1")
                                    .replaceAll(TransactionType.TRANSFER.name(), "2")
                                    .replaceAll(TransactionType.CASH_OUT.name(), "3")
                                    .replaceAll(TransactionType.DEBIT.name(), "4")
                                    .replaceAll(TransactionType.CASH_IN.name(), "5")
                                            .replaceAll("C", "1")
                                            .replaceAll("M", "2"))).toArray();
                    if (algorithmConfiguration.isMakeFeaturesMoreGaussian()) {
                        FraudDetectionAlgorithmSpark.this.makeFeaturesMoreGaussian(featureValues);
                    }
                    //always skip 9 and 10 because they are labels fraud or not fraud
                    double label = featureValues[9];
                    featureValues = Arrays.copyOfRange(featureValues, 0, 9);
                    return new LabeledPoint(label, Vectors.dense(featureValues));
                }).cache();
    }
}
