package ramo.klevis.ml.fraud;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static java.lang.Boolean.*;
import static java.lang.Integer.*;

/**
 * Created by klevis.ramo on 9/10/2017.
 */
public class Run {

    private static final String ALGORITHM_PROPERTIES_PATH = "src/main/resources/algorithm.properties";
    private static final String TRANSACTION_TYPES = "transactionTypes";
    private static final String SKIP_FEATURES = "skipFeatures";
    private static final String MAKE_FEATURES_MORE_GAUSSIAN = "makeFeaturesMoreGaussian";
    private static final String HADOOP_APPLICATION_PATH = "hadoopApplicationPath";
    private static final String FILE_NAME = "fileName";
    private static final String RUNS_TIME = "runsTime";

    public static void main(String[] args) throws IOException {
        AlgorithmConfiguration algorithmConfiguration = getAlgorithmConfigurationFromProperties();



    }

    private static AlgorithmConfiguration getAlgorithmConfigurationFromProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(new File(ALGORITHM_PROPERTIES_PATH).getAbsoluteFile()));
        AlgorithmConfiguration algorithmConfiguration = new AlgorithmConfiguration.AlgorithmConfigurationBuilder()
                .withTransactionTypes(properties.getProperty(TRANSACTION_TYPES).split(","))
                .withSkipFeatures(properties.getProperty(SKIP_FEATURES).split(","))
                .withMakeFeaturesMoreGaussian(parseBoolean(properties.getProperty(MAKE_FEATURES_MORE_GAUSSIAN)))
                .withHadoopApplicationPath(properties.getProperty(HADOOP_APPLICATION_PATH))
                .withFileName(properties.getProperty(FILE_NAME))
                .withRunsTime(parseInt(properties.getProperty(RUNS_TIME)))
                .withTrainDataNormalPercentage(parseInt(properties.getProperty("trainDataNormalPercentage")))
                .withTrainDataFraudPercentage(parseInt(properties.getProperty("trainDataFraudPercentage")))
                .withTestDataFraudPercentage(parseInt(properties.getProperty("testDataFraudPercentage")))
                .withTestDataNormalPercentage(parseInt(properties.getProperty("testDataNormalPercentage")))
                .withCrossDataFraudPercentage(parseInt(properties.getProperty("crossDataFraudPercentage")))
                .withCrossDataNormalPercentage(parseInt(properties.getProperty("crossDataNormalPercentage")))
                .createAlgorithmConfiguration();
        return algorithmConfiguration;
    }
}
