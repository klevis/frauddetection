package ramo.klevis.ml.fraud;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.feature.Normalizer;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.KernelDensity;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.distribution.MultivariateGaussian;
import org.apache.spark.mllib.util.MLUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by klevis.ramo on 9/5/2017.
 */
public class Application {

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Finance Fraud Detection").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Vector> data = loadData();
        JavaRDD<Vector> paralleledData = sc.parallelize(data);
        MultivariateStatisticalSummary summary = Statistics.colStats(paralleledData.rdd());
        System.out.println("Mean mu" + summary.mean());  // a dense vector containing the mean value for each column
        System.out.println("Sigma " + summary.variance());
        MultivariateGaussian multivariateGaussian = new MultivariateGaussian(summary.mean(), DenseMatrix.diag(summary.variance()));
        List<Double> map = paralleledData.map(e -> multivariateGaussian.logpdf(e)).collect();
        System.out.println();

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
            data.add(Vectors.dense(as));
        }
        return data;
    }
}
