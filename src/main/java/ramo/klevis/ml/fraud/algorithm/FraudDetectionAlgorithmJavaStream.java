package ramo.klevis.ml.fraud.algorithm;

import org.apache.spark.api.java.JavaSparkContext;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Created by klevis.ramo on 9/5/2017.
 */
public class FraudDetectionAlgorithmJavaStream extends AlgorithmTemplateExecution<List<LabeledPoint>> {


    public FraudDetectionAlgorithmJavaStream(AlgorithmConfiguration algorithmConfiguration) {
        super(algorithmConfiguration);
    }

    @Override
    protected List<LabeledPoint> getTestData(GeneratedData<List<LabeledPoint>> crossData) {
        return Stream.concat(crossData.leftRegular.stream(), crossData.leftAnomalies.stream()).collect(toList());
    }

    @Override
    protected MultivariateStatisticalSummary getMultivariateSummary(GeneratedData<List<LabeledPoint>> trainData) {
        return Statistics.colStats(sparkContext.parallelize(trainData.regularAndAnomalyData).map(e -> e.features()).rdd());
    }

    @Override
    protected void setTestDataSizes(ResultsSummary resultsSummary, GeneratedData<List<LabeledPoint>> crossData) {
        resultsSummary.setTestRegularSize(crossData.leftRegular.size());
        resultsSummary.setTestFraudSize(crossData.leftAnomalies.size());
        resultsSummary.setTestTotalDataSize(crossData.leftRegular.size() + crossData.leftAnomalies.size());
    }

    @Override
    protected void setCrossDataSizes(ResultsSummary resultsSummary, GeneratedData<List<LabeledPoint>> crossData) {
        resultsSummary.setCrossRegularSize(crossData.regular.size());
        resultsSummary.setCrossFraudSize(crossData.anomaly.size());
        resultsSummary.setCrossTotalDataSize(crossData.regularAndAnomalyData.size());

    }

    @Override
    protected void setTrainDataSizes(ResultsSummary resultsSummary, GeneratedData<List<LabeledPoint>> trainData) {
        resultsSummary.setTrainRegularSize(trainData.regular.size());
        resultsSummary.setTrainFraudSize(trainData.anomaly.size());
        resultsSummary.setTrainTotalDataSize(trainData.regularAndAnomalyData.size());
    }

    @Override
    protected long getTotalAnomalies(List<LabeledPoint> anomalies) {
        return anomalies.size();
    }

    @Override
    protected long getTotalRegular(List<LabeledPoint> regular) {
        return regular.size();
    }

    protected List<LabeledPoint> filterAnomaliesData(List<LabeledPoint> filteredDataByType) {
        List<LabeledPoint> anomaliesRDD = filteredDataByType.stream().parallel().filter(e -> e.label() == (1d)).collect(toList());
        return anomaliesRDD;
    }

    protected List<LabeledPoint> filterRegularData(List<LabeledPoint> filteredDataByType) {
        List<LabeledPoint> regularDataRDD = filteredDataByType.stream().parallel().filter(e -> e.label() == (0d)).collect(toList());
        return regularDataRDD;
    }


    protected TestResult testAlgorithmWithData(JavaSparkContext sc, List<LabeledPoint> testData, MultivariateStatisticalSummary summary, Double bestEpsilon) {
        MultivariateGaussian multivariateGaussian = new MultivariateGaussian(summary.mean(), DenseMatrix.diag(summary.variance()));
        List<Tuple<Double, Double>> testDataProbabilityDenseFunction = testData.stream().parallel().map(e -> new Tuple<>(e.label(), multivariateGaussian.logpdf(e.features()))).collect(toList());
        long totalFrauds = testDataProbabilityDenseFunction.stream().parallel().filter(e -> e.getLabel().equals(Double.valueOf(1))).count();
        long foundFrauds = testDataProbabilityDenseFunction.stream().parallel().filter(e -> e.getValue() < bestEpsilon
                && e.getLabel().equals(Double.valueOf(1))).count();

        long flaggedFrauds = testDataProbabilityDenseFunction.stream().parallel().filter(e -> e.getValue() < bestEpsilon).count();

        long missedFrauds = testDataProbabilityDenseFunction.stream().parallel().filter(e -> e.getValue() > bestEpsilon
                && e.getLabel().equals(Double.valueOf(1))).count();

        return new TestResult(totalFrauds, foundFrauds, flaggedFrauds, missedFrauds);
    }

    protected GeneratedData<List<LabeledPoint>> randomlyGenerateData(int normalSize, int fraudSize, List<LabeledPoint> regularData, List<LabeledPoint> anomalies, JavaSparkContext sparkContext) {
        Collections.shuffle(regularData);
        Collections.shuffle(anomalies);
        List<LabeledPoint> regular = regularData.stream().parallel().limit(normalSize).collect(toList());
        List<LabeledPoint> fraud = anomalies.stream().parallel().limit(fraudSize).collect(toList());
        ArrayList<LabeledPoint> all = new ArrayList<>();
        all.addAll(regular);
        all.addAll(fraud);
        List<LabeledPoint> leftRegular = regularData.stream().parallel().skip(normalSize).collect(toList());
        List<LabeledPoint> leftAnomalies = anomalies.stream().parallel().skip(fraudSize).collect(toList());
        return new GeneratedData<>(regular, fraud, all, leftRegular, leftAnomalies);
    }

    protected Double findBestEpsilon(JavaSparkContext sc, GeneratedData<List<LabeledPoint>> generatedData, MultivariateStatisticalSummary summary) {
        MultivariateGaussian multivariateGaussian = new MultivariateGaussian(summary.mean(), DenseMatrix.diag(summary.variance()));
        List<Tuple<Double, Double>> trainDataProbabilityDenseFunction = sc.parallelize(generatedData.regularAndAnomalyData).map(e -> new Tuple<>(e.label(), multivariateGaussian.logpdf(e.features()))).collect();
        Double min = trainDataProbabilityDenseFunction.stream().parallel().min(new SerializableTupleComparator()).get().getValue();
        Double max = trainDataProbabilityDenseFunction.stream().parallel().max(new SerializableTupleComparator()).get().getValue();
        Double step = (max - min) / 1000d;
        List<Double> epsilons = new ArrayList<>();
        for (double epsilon = min; epsilon < max; epsilon = epsilon + step) {
            epsilons.add(epsilon);
        }
        List<Tuple<Double, Double>> trainDataProbabilityDenseFunctionList = trainDataProbabilityDenseFunction;
        return epsilons.stream().parallel().reduce((e1, e2) -> {
                    double f11 = getF1(trainDataProbabilityDenseFunctionList, e1);
                    double f12 = getF1(trainDataProbabilityDenseFunctionList, e2);
                    if (f11 > f12) {
                        return e1;
                    } else {
                        return e2;
                    }
                }
        ).get();
    }

    private double getF1(List<Tuple<Double, Double>> trainDataProbabilityDenseFunctionList, Double epsilon) {
        long successfullyDetectedFrauds = trainDataProbabilityDenseFunctionList.stream().parallel()
                .filter(e -> e.getValue() <= epsilon
                        && e.getLabel().equals(Double.valueOf(1))).count();

        long wronglyFlaggedAsFrauds = trainDataProbabilityDenseFunctionList.stream().parallel()
                .filter(e -> e.getValue() <= epsilon
                        && e.getLabel().equals(Double.valueOf(0))).count();

        long missedFrauds = trainDataProbabilityDenseFunctionList.stream().parallel()
                .filter(e -> e.getValue() > epsilon
                        && e.getLabel().equals(Double.valueOf(1))).count();
        double prec = (double) successfullyDetectedFrauds / (double) (successfullyDetectedFrauds + wronglyFlaggedAsFrauds);

        double rec = (double) successfullyDetectedFrauds / (double) (successfullyDetectedFrauds + missedFrauds);
        return 2 * (prec * rec) / (prec + rec);
    }

    protected List<LabeledPoint> filterRequestedDataType(List<LabeledPoint> data, List<TransactionType> types, List<Integer> skipFeatures, JavaSparkContext sc) throws IOException {
        return data.stream().parallel().filter(e -> e != null &&
                types.stream().filter(type -> type == TransactionType.ALL ||
                        type.getTransactionType() == e.features().apply(1)).findAny().isPresent())
                .map(e -> skipSelectedFeatures(e, skipFeatures)).collect(toList());
    }

    protected List<LabeledPoint> loadDataFromFile(JavaSparkContext sc) throws IOException {
        File file = new File(algorithmConfiguration.getFileName());
        return Files.readAllLines(Paths.get(file.getAbsolutePath())).parallelStream().skip(1).map(line -> {
            line = line.replace(TransactionType.PAYMENT.name(), "1")
                    .replace(TransactionType.TRANSFER.name(), "2")
                    .replace(TransactionType.CASH_OUT.name(), "3")
                    .replace(TransactionType.DEBIT.name(), "4")
                    .replace(TransactionType.CASH_IN.name(), "5")
                    .replace("C", "1")
                    .replace("M", "2");
            double[] as = Stream.of(line.split(","))
                    .mapToDouble(e -> Double.parseDouble(e)).toArray();
            if (algorithmConfiguration.isMakeFeaturesMoreGaussian()) {
                makeFeaturesMoreGaussian(as);
            }
            double[] doubles = Arrays.copyOfRange(as, 0, 9);//skip 9 and 10 for frauds}
            return new LabeledPoint(as[9], Vectors.dense(doubles));

        }).collect(toList());
    }


}
