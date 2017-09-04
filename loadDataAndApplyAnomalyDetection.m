function loadDataAndApplyAnomalyDetection()
data=csvread("data/prototypeData.csv");
applyAnomalyDetection(data,2000,100000,2097,100000);
end;