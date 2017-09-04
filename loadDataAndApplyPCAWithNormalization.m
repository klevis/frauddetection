function loadDataAndApplyPCAWithNormalization()

data=csvread("data/prototypeData.csv");
internalPCAAndPlot(data,true);
end