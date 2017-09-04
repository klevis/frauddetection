function applyAnomalyDetectionWithGaussianFeatures(data,crossFraudSize,crossNormalSize,testFraudSize,testNormalSize)
#make feature look more like bell shaped gaussian function
data=data.^[0.5,0.1 ,0.3,0.1,0.08 ,0.3 ,0.1 ,0.1,1,1];
applyAnomalyDetection(data,crossFraudSize,crossNormalSize,testFraudSize,testNormalSize);
end;