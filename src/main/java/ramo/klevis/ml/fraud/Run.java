package ramo.klevis.ml.fraud;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;

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

    public static void main(String[] args) throws Exception {
        AlgorithmConfiguration algorithmConfiguration = getAlgorithmConfigurationFromProperties();
        setHadoopHomeEnvironmentVariable(algorithmConfiguration);
        FraudDetectionAlgorithm fraudDetectionAlgorithm = new FraudDetectionAlgorithm(algorithmConfiguration);
        List<ResultsSummary> resultsSummaries = fraudDetectionAlgorithm.executeAlgorithm();
        for (ResultsSummary resultsSummary : resultsSummaries) {
            System.out.println(resultsSummary);
        }

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

    private static void setHadoopHomeEnvironmentVariable(AlgorithmConfiguration algorithmConfiguration) throws Exception {
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
}
