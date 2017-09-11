package ramo.klevis.ml.fraud.algorithm;

import ramo.klevis.ml.fraud.data.ResultsSummary;

import java.io.IOException;
import java.util.List;

/**
 * Created by klevis.ramo on 9/11/2017.
 */
public interface IFraudDetectionAlgorithm {
    List<ResultsSummary> executeAlgorithm() throws IOException;
}
