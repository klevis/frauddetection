package ramo.klevis.ml.fraud;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Created by klevis.ramo on 9/5/2017.
 */
public class Application {

    protected static final int FRAUD_COLUMN = 8;

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Finance Fraud Detection").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Vector> data = loadData();

        List<Vector> regularData = data.stream().parallel().filter(e -> ((Double) e.apply(FRAUD_COLUMN)).equals(Double.valueOf(0))).collect(toList());
        List<Vector> anomalies = data.stream().parallel().filter(e -> ((Double) e.apply(FRAUD_COLUMN)).equals(Double.valueOf(1))).collect(toList());
        //randomize anomalies
        Collections.shuffle(anomalies);
        //randomize regular
        Collections.shuffle(regularData);

        //choose 60% as train data with no anomalies
        int trainingDataSize = (int) (0.6 * regularData.size());
        List<Vector> trainData = generateTrainData(regularData, trainingDataSize);
        ArrayList<Vector> crossData = generateCrossData(regularData, anomalies, trainingDataSize);
        List<Vector> testData = generateTestData(regularData, anomalies, trainingDataSize);

        JavaRDD<Vector> paralleledTrainData = sc.parallelize(trainData);
        MultivariateStatisticalSummary summary = Statistics.colStats(paralleledTrainData.rdd());
        System.out.println("Mean mu" + summary.mean());  // a dense vector containing the mean value for each column
        System.out.println("Sigma " + summary.variance());

        Double bestEpsilon = findBestEpsilon(sc, crossData, summary);

        test(sc, testData, summary, bestEpsilon);
        test(sc, crossData, summary, bestEpsilon);

    }

    private static void test(JavaSparkContext sc, List<Vector> testData, MultivariateStatisticalSummary summary, Double bestEpsilon) {
        JavaRDD<Vector> paralleledTestData = sc.parallelize(testData);
        MultivariateGaussian multivariateGaussian = new MultivariateGaussian(summary.mean(), DenseMatrix.diag(summary.variance()));
        List<Double> testDataProbabilityDenseFunction = paralleledTestData.map(e -> multivariateGaussian.logpdf(e)).collect();
        long foundFrauds = IntStream.range(0, testDataProbabilityDenseFunction.size()).parallel().
                filter(index -> testDataProbabilityDenseFunction.get(index) < bestEpsilon
                        && ((Double) testData.get(index).apply(FRAUD_COLUMN)).equals(Double.valueOf(1))).count();

        long flaggedFrauds = IntStream.range(0, testDataProbabilityDenseFunction.size()).parallel().
                filter(index -> testDataProbabilityDenseFunction.get(index) < bestEpsilon).count();

        long missedFrauds = IntStream.range(0, testDataProbabilityDenseFunction.size()).parallel().
                filter(index -> testDataProbabilityDenseFunction.get(index) > bestEpsilon
                        && ((Double) testData.get(index).apply(FRAUD_COLUMN)).equals(Double.valueOf(1))).count();

        System.out.println("foundFrauds = " + foundFrauds);
        System.out.println("flaggedFrauds = " + flaggedFrauds);
        System.out.println("missedFrauds = " + missedFrauds);
    }

    private static Double findBestEpsilon(JavaSparkContext sc, ArrayList<Vector> crossData, MultivariateStatisticalSummary summary) {
        JavaRDD<Vector> paralleledCrossData = sc.parallelize(crossData);
        MultivariateGaussian multivariateGaussian = new MultivariateGaussian(summary.mean(), DenseMatrix.diag(summary.variance()));
        List<Double> trainDataProbabilityDenseFunction = paralleledCrossData.map(e -> multivariateGaussian.logpdf(e)).collect();
        ArrayList<Double> sorted = new ArrayList<>(trainDataProbabilityDenseFunction);
        Collections.sort(sorted);
        Double min = sorted.get(0);
        Double max = sorted.get(trainDataProbabilityDenseFunction.size() - 1);
        Double step = (max - min) / 1000d;
        Double bestEpsilon = Double.MAX_VALUE;
        Double bestF1 = Double.MIN_VALUE;
        long bsuccessfullyDetectedFrauds = 0;
        long bwronglyFlaggedAsFrauds = 0;
        long bmissedFrauds = 0;
        for (double epsilon = min; epsilon < max; epsilon = epsilon + step) {
            double finalEpsilon = epsilon;
            long successfullyDetectedFrauds = IntStream.range(0, trainDataProbabilityDenseFunction.size()).parallel()
                    .filter(index -> trainDataProbabilityDenseFunction.get(index) <= finalEpsilon
                            && ((Double) crossData.get(index).apply(FRAUD_COLUMN)).equals(Double.valueOf(1))).count();

            long wronglyFlaggedAsFrauds = IntStream.range(0, trainDataProbabilityDenseFunction.size()).parallel()
                    .filter(index -> trainDataProbabilityDenseFunction.get(index) <= finalEpsilon
                            && ((Double) crossData.get(index).apply(FRAUD_COLUMN)).equals(Double.valueOf(0))).count();

            long missedFrauds = IntStream.range(0, trainDataProbabilityDenseFunction.size()).parallel()
                    .filter(index -> trainDataProbabilityDenseFunction.get(index) > finalEpsilon
                            && ((Double) crossData.get(index).apply(FRAUD_COLUMN)).equals(Double.valueOf(1))).count();

            double prec = (double) successfullyDetectedFrauds / (double) (successfullyDetectedFrauds + wronglyFlaggedAsFrauds);

            double rec = (double) successfullyDetectedFrauds / (double) (successfullyDetectedFrauds + missedFrauds);
            double f1 = 2 * prec * rec / (prec + rec);
            if (f1 > bestF1) {
                bestF1 = f1;
                bestEpsilon = epsilon;
                bsuccessfullyDetectedFrauds = successfullyDetectedFrauds;
                bwronglyFlaggedAsFrauds = wronglyFlaggedAsFrauds;
                bmissedFrauds = missedFrauds;
            }
        }
        System.out.println("bmissedFrauds = " + bmissedFrauds);
        System.out.println("bwronglyFlaggedAsFrauds = " + bwronglyFlaggedAsFrauds);
        System.out.println("bsuccessfullyDetectedFrauds = " + bsuccessfullyDetectedFrauds);
        System.out.println("bF1 = " + bestF1);
        System.out.println("bestEpsilon = " + bestEpsilon);
        return bestEpsilon;
    }

    private static List<Vector> generateTrainData(List<Vector> regularData, int trainingDataSize) {
        return regularData.stream().parallel().limit(trainingDataSize).collect(toList());
    }

    private static List<Vector> generateTestData(List<Vector> regularData, List<Vector> anomalies, int trainingDataSize) {
        int crossRegularDataSize = (int) ((regularData.size() - trainingDataSize) * 0.5);
        //choose the rest as test validation data with no anomalies
        List<Vector> testDataRegular = regularData.stream().parallel()
                .skip(trainingDataSize + crossRegularDataSize)
                .limit(regularData.size() - trainingDataSize + crossRegularDataSize).collect(toList());
        List<Vector> testAnomalies = anomalies.stream().skip(anomalies.size() / 2).limit(anomalies.size() - (anomalies.size() / 2)).collect(toList());
        List<Vector> testData = new ArrayList<>();
        testData.addAll(testDataRegular);
        testData.addAll(testAnomalies);
        return testData;
    }

    private static ArrayList<Vector> generateCrossData(List<Vector> regularData, List<Vector> anomalies, int trainingDataSize) {
        //choose 20% as cross validation data with no anomalies
        int crossRegularDataSize = (int) ((regularData.size() - trainingDataSize) * 0.5);
        List<Vector> crossDataRegular = regularData.stream().parallel().skip(trainingDataSize).limit(crossRegularDataSize).collect(toList());
        List<Vector> crossDataAnomalies = anomalies.stream().limit(anomalies.size() / 2).collect(toList());
        ArrayList<Vector> crossData = new ArrayList<>();
        crossData.addAll(crossDataRegular);
        crossData.addAll(crossDataAnomalies);
        return crossData;
    }


    private static List<Vector> loadData() throws IOException {
        File file = new File("data/prototypeData.csv");
        FileReader in = new FileReader(file);
        BufferedReader br = new BufferedReader(in);
        String line;
        List<Vector> data = new ArrayList<Vector>();
        br.readLine();
        while ((line = br.readLine()) != null) {
            double[] as = Stream.of(line.split(",")).mapToDouble(e -> Double.parseDouble(e)).toArray();
            double[] power = {0.5, 0.1, 0.3, 0.1, 0.08, 0.3, 0.1, 0.1, 1, 1};
            for (int i = 0; i < as.length; i++) {
                as[i] = Math.pow(as[i], power[i]);
            }
            data.add(Vectors.dense(as));
        }
        return data;
    }
}
