function loadDataAndApplyAnomalyDetectionWithGaussianFeatures()
data=csvread("data/prototypeData.csv");
#make feature look more like bell shaped gaussian function
data=data.^[0.5,0.1 ,0.3,0.1,0.08 ,0.3 ,0.1 ,0.1,1,1];
applyAnomalyDetection(data,2000,100000,2097,100000);
end;