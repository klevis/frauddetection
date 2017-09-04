function [mu sigma2 epsilon]=trainAndFindBestEpsilon(trainData,dataCross,crossFraudResults)

[mu sigma2] = findMuAndSigma(trainData);


trainFunctionResults = multivariateGaussianFunction(trainData, mu, sigma2);


crossFunctionResults = multivariateGaussianFunction(dataCross, mu, sigma2);

%  Find the best epsilon base onf F1
[epsilon F1] = findBestEpsilon(dataCross,crossFraudResults,crossFunctionResults);
epsilon
F1
anomalies=sum(trainFunctionResults < epsilon)
end