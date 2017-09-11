package ramo.klevis.ml.fraud.algorithm;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import ramo.klevis.ml.fraud.data.GeneratedData;
import ramo.klevis.ml.fraud.data.ResultsSummary;
import ramo.klevis.ml.fraud.data.TestResult;
import ramo.klevis.ml.fraud.data.TransactionType;
import ramo.klevis.ml.fraud.data.Tuple;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by klevis.ramo on 9/5/2017.
 */
public abstract class AlgorithmTemplateExecution<T> implements Serializable, IFraudDetectionAlgorithm {


    protected final AlgorithmConfiguration algorithmConfiguration;
    protected JavaSparkContext sparkContext;

    public AlgorithmTemplateExecution(AlgorithmConfiguration algorithmConfiguration) {
        this.algorithmConfiguration = algorithmConfiguration;
    }

    @Override
    public List<ResultsSummary> executeAlgorithm() throws IOException {
        sparkContext = createSparkContext();
        T labeledPointJavaRDD = loadDataFromFile(sparkContext);
        List<Integer> skipFeatures = algorithmConfiguration.getSkipFeatures();
        List<TransactionType> transactionTypesToExecute = algorithmConfiguration.getTransactionTypesToExecute();
        List<ResultsSummary> resultsSummaries = new ArrayList<>();
        int runsTime = algorithmConfiguration.getRunsTime();
        for (int i = 0; i < runsTime; i++) {
            long startTime = System.currentTimeMillis();
            T filterRequestedByDataType = filterRequestedDataType(labeledPointJavaRDD, transactionTypesToExecute, skipFeatures, sparkContext);
            ResultsSummary resultsSummary = runAnomalyDetection(sparkContext, filterRequestedByDataType);
            resultsSummary.setId(i);
            resultsSummary.setTransactionTypes(transactionTypesToExecute);
            resultsSummary.setAlgorithmConfiguration(algorithmConfiguration);
            resultsSummary.setTimeInMilliseconds((System.currentTimeMillis()) - startTime);
            resultsSummaries.add(resultsSummary);
        }
        return resultsSummaries;
    }

    private ResultsSummary runAnomalyDetection(JavaSparkContext sc, T filteredDataByType) throws IOException {

        ResultsSummary resultsSummary = new ResultsSummary();
        T regular = filterRegularData(filteredDataByType);
        T anomalies = filterAnomaliesData(filteredDataByType);

        long totalRegularSize = getTotalRegular(regular);
        long totalAnomaliesSize = getTotalAnomalies(anomalies);
        resultsSummary.setTotalRegularSize(totalRegularSize);
        resultsSummary.setTotalFraudSize(totalAnomaliesSize);


        GeneratedData<T> trainData = randomlyGenerateData((int) (algorithmConfiguration.getTrainDataNormalPercentage() * totalRegularSize),
                (int) (algorithmConfiguration.getTrainDataFraudPercentage() * totalAnomaliesSize), regular, anomalies, sc);
        setTrainDataSizes(resultsSummary, trainData);

        GeneratedData<T> crossData = randomlyGenerateData((int) (algorithmConfiguration.getCrossDataNormalPercentage() * totalRegularSize),
                (int) (algorithmConfiguration.getCrossDataFraudPercentage() * totalAnomaliesSize), trainData.leftRegular, trainData.leftAnomalies, sc);
        setCrossDataSizes(resultsSummary, crossData);

        setTestDataSizes(resultsSummary, crossData);

        MultivariateStatisticalSummary summary = getMultivariateSummary(trainData);

        resultsSummary.setMean(summary.mean().toArray());
        resultsSummary.setSigma(summary.variance().toArray());

        Double bestEpsilon = findBestEpsilon(sc, crossData, summary);
        resultsSummary.setEpsilon(bestEpsilon);

        TestResult testResultFromTestData = testAlgorithmWithData(sc, getTestData(crossData), summary, bestEpsilon);
        fillTestDataResults(resultsSummary, testResultFromTestData);

        TestResult testResultFromCrossData = testAlgorithmWithData(sc, crossData.regularAndAnomalyData, summary, bestEpsilon);
        fillCrossDataResults(resultsSummary, testResultFromCrossData);
        return resultsSummary;
    }

    protected abstract T getTestData(GeneratedData<T> crossData);

    protected abstract MultivariateStatisticalSummary getMultivariateSummary(GeneratedData<T> trainData);

    protected abstract void setTestDataSizes(ResultsSummary resultsSummary, GeneratedData<T> crossData);

    protected abstract void setCrossDataSizes(ResultsSummary resultsSummary, GeneratedData<T> crossData);

    protected abstract void setTrainDataSizes(ResultsSummary resultsSummary, GeneratedData<T> trainData);

    protected abstract long getTotalAnomalies(T anomalies);

    protected abstract long getTotalRegular(T regular);

    protected abstract T filterAnomaliesData(T filteredDataByType);

    protected abstract T filterRegularData(T filteredDataByType);

    protected abstract TestResult testAlgorithmWithData(JavaSparkContext sc, T testData, MultivariateStatisticalSummary summary, Double bestEpsilon);

    protected abstract Double findBestEpsilon(JavaSparkContext sc, GeneratedData<T> generatedData, MultivariateStatisticalSummary summary);


    protected abstract GeneratedData<T> randomlyGenerateData(int normalSize, int fraudSize, T regularData, T anomalies, JavaSparkContext sparkContext);


    protected abstract T filterRequestedDataType(T data, List<TransactionType> type, List<Integer> skipFeatures, JavaSparkContext sc) throws IOException;

    protected abstract T loadDataFromFile(JavaSparkContext sc) throws IOException;

    private void fillCrossDataResults(ResultsSummary resultsSummary, TestResult testResultFromCrossData) {
        resultsSummary.setCrossFoundFraudSize(testResultFromCrossData.getFoundFrauds());
        resultsSummary.setCrossFlaggedAsFraud(testResultFromCrossData.getFlaggedFrauds());
        resultsSummary.setCrossNotFoundFraudSize(testResultFromCrossData.getMissedFrauds());
        resultsSummary.setCrossFraudSize(testResultFromCrossData.getTotalFrauds());
    }

    private void fillTestDataResults(ResultsSummary resultsSummary, TestResult testResultFromTestData) {
        resultsSummary.setTestFoundFraudSize(testResultFromTestData.getFoundFrauds());
        resultsSummary.setTestFlaggedAsFraud(testResultFromTestData.getFlaggedFrauds());
        resultsSummary.setTestNotFoundFraudSize(testResultFromTestData.getMissedFrauds());
        resultsSummary.setTestFraudSize(testResultFromTestData.getTotalFrauds());
    }

    private JavaSparkContext createSparkContext() {
        SparkConf conf = new SparkConf().setAppName("Finance Fraud Detection").setMaster("local[*]");
        return new JavaSparkContext(conf);
    }


    protected void makeFeaturesMoreGaussian(double[] featureValues) {
        double[] powers = {0.5, 1, 0.1, 0.3, 0.1, 0.08, 0.3, 0.1, 0.1, 1, 1};
        for (int i = 0; i < featureValues.length; i++) {
            featureValues[i] = Math.pow(featureValues[i], powers[i]);
        }
    }

    protected LabeledPoint skipSelectedFeatures(LabeledPoint labeledPoint, List<Integer> skipFeatures) {
        double[] featureValues = labeledPoint.features().toArray();
        double[] finalFeatureValues = new double[featureValues.length - skipFeatures.size()];
        int index = 0;
        for (int i = 0; i < featureValues.length; i++) {
            if (!skipFeatures.contains(i)) {
                finalFeatureValues[index++] = featureValues[i];
            }
        }
        return new LabeledPoint(labeledPoint.label(), Vectors.dense(finalFeatureValues));
    }

    protected class SerializableTupleComparator implements Comparator<Tuple<Double, Double>>, Serializable {
        @Override
        public int compare(Tuple<Double, Double> o1, Tuple<Double, Double> o2) {
            return o1.getValue().compareTo(o2.getValue());
        }
    }
}
