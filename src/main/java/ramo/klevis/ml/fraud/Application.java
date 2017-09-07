package ramo.klevis.ml.fraud;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Created by klevis.ramo on 9/5/2017.
 */
public class Application {

    //    protected static final String DATA_CSV = "prototypeData.csv";
    protected static final String DATA_CSV = "allData.csv";
    private static long totalFoundFrauds = 0;
    private static long totalMissedFrauds = 0;
    private static long totalFrauds = 0;

    public static void main(String[] args) throws IOException {


        SparkConf conf = new SparkConf().setAppName("Finance Fraud Detection").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<LabeledPoint> data = loadData();

        List<LabeledPoint> regularData = data.stream().parallel().filter(e -> e.label() == (0d)).collect(toList());
        List<LabeledPoint> anomalies = data.stream().parallel().filter(e -> e.label() == (1d)).collect(toList());
        totalFrauds = totalFrauds + anomalies.size();
        System.out.println("anomalies.size() = " + anomalies.size());
        //randomize anomalies
        Collections.shuffle(anomalies);
        //randomize regular
        Collections.shuffle(regularData);

        //choose 60% as train data with no anomalies
        int trainingDataSize = (int) (0.6 * regularData.size());
        List<LabeledPoint> trainData = generateTrainData(regularData, trainingDataSize);
        List<LabeledPoint> crossData = generateCrossData(regularData, anomalies, trainingDataSize);
        List<LabeledPoint> testData = generateTestData(regularData, anomalies, trainingDataSize);

        JavaRDD<LabeledPoint> paralleledTrainData = sc.parallelize(trainData);
        MultivariateStatisticalSummary summary = Statistics.colStats(paralleledTrainData.map(e -> e.features()).rdd());
        System.out.println("Mean mu" + summary.mean());  // a dense vector containing the mean value for each column
        System.out.println("Sigma " + summary.variance());

        Double bestEpsilon = findBestEpsilon(sc, crossData, summary);

        test(sc, testData, summary, bestEpsilon);
        test(sc, crossData, summary, bestEpsilon);

    }

    private static void test(JavaSparkContext sc, List<LabeledPoint> testData, MultivariateStatisticalSummary summary, Double bestEpsilon) {
        JavaRDD<LabeledPoint> paralleledTestData = sc.parallelize(testData);
        MultivariateGaussian multivariateGaussian = new MultivariateGaussian(summary.mean(), DenseMatrix.diag(summary.variance()));
        List<Tuple<Double, Double>> testDataProbabilityDenseFunction = paralleledTestData.map(e -> new Tuple<>(e.label(), multivariateGaussian.logpdf(e.features()))).collect();
        JavaRDD<Tuple<Double, Double>> parallelingTestDataProbability = sc.parallelize(testDataProbabilityDenseFunction);

        long totalFrauds = testDataProbabilityDenseFunction.stream().parallel().filter(e -> e.label.equals(Double.valueOf(1))).count();
        long foundFrauds = testDataProbabilityDenseFunction.stream().filter(e -> e.value < bestEpsilon
                && e.label.equals(Double.valueOf(1))).count();

        long flaggedFrauds = testDataProbabilityDenseFunction.stream().parallel().filter(e -> e.value < bestEpsilon).count();

        long missedFrauds = testDataProbabilityDenseFunction.stream().parallel().filter(e -> e.value > bestEpsilon
                && e.label.equals(Double.valueOf(1))).count();

        totalFoundFrauds = totalFoundFrauds + foundFrauds;
        totalMissedFrauds = totalMissedFrauds + missedFrauds;
        System.out.println("foundFrauds = " + foundFrauds + " from total " + totalFrauds + " -> " + (((double)foundFrauds /(double) totalFrauds) * 100));
        System.out.println("flaggedFrauds = " + flaggedFrauds);
        System.out.println("missedFrauds = " + missedFrauds);
    }

    private static Double findBestEpsilon(JavaSparkContext sc, List<LabeledPoint> crossData, MultivariateStatisticalSummary summary) {
        JavaRDD<LabeledPoint> paralleledCrossData = sc.parallelize(crossData);
        MultivariateGaussian multivariateGaussian = new MultivariateGaussian(summary.mean(), DenseMatrix.diag(summary.variance()));
        List<Tuple<Double, Double>> trainDataProbabilityDenseFunction = paralleledCrossData.map(e -> new Tuple<>(e.label(), multivariateGaussian.logpdf(e.features()))).collect();
        JavaRDD<Tuple<Double, Double>> parallelizeTrainDataProbability = sc.parallelize(trainDataProbabilityDenseFunction);
        ArrayList<Tuple<Double, Double>> sorted = new ArrayList<>(trainDataProbabilityDenseFunction);
        Collections.sort(sorted, Comparator.comparing(o -> o.value));
        Double min = sorted.get(0).value;
        Double max = sorted.get(trainDataProbabilityDenseFunction.size() - 1).value;
        Double step = (max - min) / 1000d;
        Double bestEpsilon = Double.MAX_VALUE;
        Double bestF1 = Double.MIN_VALUE;
        long bsuccessfullyDetectedFrauds = 0;
        long bwronglyFlaggedAsFrauds = 0;
        long bmissedFrauds = 0;
        for (double epsilon = min; epsilon < max; epsilon = epsilon + step) {
            double finalEpsilon = epsilon;
            long successfullyDetectedFrauds = trainDataProbabilityDenseFunction.stream().parallel()
                    .filter(e -> e.value <= finalEpsilon
                            && e.label.equals(Double.valueOf(1))).count();

            long wronglyFlaggedAsFrauds = trainDataProbabilityDenseFunction.stream().parallel()
                    .filter(e -> e.value <= finalEpsilon
                            && e.label.equals(Double.valueOf(0))).count();

            long missedFrauds = trainDataProbabilityDenseFunction.stream().parallel()
                    .filter(e -> e.value > finalEpsilon
                            && e.label.equals(Double.valueOf(1))).count();

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

    private static List<LabeledPoint> generateTrainData(List<LabeledPoint> regularData, int trainingDataSize) {
        return regularData.stream().parallel().limit(trainingDataSize).collect(toList());
    }

    private static List<LabeledPoint> generateTestData(List<LabeledPoint> regularData, List<LabeledPoint> anomalies, int trainingDataSize) {
        int crossRegularDataSize = (int) ((regularData.size() - trainingDataSize) * 0.5);
        //choose the rest as test validation data with no anomalies
        List<LabeledPoint> testDataRegular = regularData.stream().parallel()
                .skip(trainingDataSize + crossRegularDataSize)
                .limit(regularData.size() - trainingDataSize + crossRegularDataSize).collect(toList());
        List<LabeledPoint> testAnomalies = anomalies.stream().skip(anomalies.size() / 2).limit(anomalies.size() - (anomalies.size() / 2)).collect(toList());
        List<LabeledPoint> testData = new ArrayList<>();
        testData.addAll(testDataRegular);
        testData.addAll(testAnomalies);
        return testData;
    }

    private static ArrayList<LabeledPoint> generateCrossData(List<LabeledPoint> regularData, List<LabeledPoint> anomalies, int trainingDataSize) {
        //choose 20% as cross validation data with no anomalies
        int crossRegularDataSize = (int) ((regularData.size() - trainingDataSize) * 0.5);
        List<LabeledPoint> crossDataRegular = regularData.stream().parallel().skip(trainingDataSize).limit(crossRegularDataSize).collect(toList());
        List<LabeledPoint> crossDataAnomalies = anomalies.stream().limit(anomalies.size() / 2).collect(toList());
        ArrayList<LabeledPoint> crossData = new ArrayList<>();
        crossData.addAll(crossDataRegular);
        crossData.addAll(crossDataAnomalies);
        return crossData;
    }


    private static List<LabeledPoint> loadData() throws IOException {
        File file = new File("data/" + DATA_CSV);
        FileReader in = new FileReader(file);
        BufferedReader br = new BufferedReader(in);
        String line;
        List<LabeledPoint> data = new ArrayList<LabeledPoint>();
        //skip first line
        br.readLine();
        while ((line = br.readLine()) != null) {
            double[] as = Stream.of(line.split(",")).mapToDouble(e -> Double.parseDouble(e)).toArray();
//            double[] power = {0.5, 0.05, 0.1, 0.3, 0.1, 0.08, 0.3, 0.1, 0.1, 1, 1};
//            for (int i = 0; i < as.length; i++) {
//                as[i] = Math.pow(as[i], power[i]);
//            }
            double[] doubles1 = Arrays.copyOfRange(as, 0, 1);
            double[] doubles2 = Arrays.copyOfRange(as, 2, 9);
//            if (as[1] == 2d)
                data.add(new LabeledPoint(as[9], Vectors.dense(ArrayUtils.addAll(doubles1, doubles2))));
        }
        return data;
    }

    static class Tuple<F, S> implements Serializable {
        private F label;
        private S value;

        public Tuple(F label, S value) {

            this.label = label;
            this.value = value;
        }
    }
}
