package ramo.klevis.ml.fraud.data;

public class GeneratedData<T> {
    final public T regular;
    final public T anomaly;
    final public T regularAndAnomalyData;
    final public T leftRegular;
    final public T leftAnomalies;

    public GeneratedData(T regular, T anomaly, T regularAndAnomalyData, T leftRegular, T leftAnomalies) {
        this.regular = regular;
        this.anomaly = anomaly;
        this.regularAndAnomalyData = regularAndAnomalyData;
        this.leftRegular = leftRegular;
        this.leftAnomalies = leftAnomalies;
    }

}