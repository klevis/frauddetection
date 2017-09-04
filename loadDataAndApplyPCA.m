function loadDataAndApplyPCA()

data=csvread("data/prototypeData.csv");
internalPCAAndPlot(data,false);
end