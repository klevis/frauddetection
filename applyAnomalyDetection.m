function applyAnomalyDetection(data,crossFraudSize,crossNormalSize,testFraudSize,testNormalSize)
#remove first row as is header
data=data(2:size(data,1),:);
fraudCol=9;
selectedColumns=[1,2,3,4,5,6,7,8];

#plot data before algorithm is applied
internalPlotDataBeforeAnomalyDetection(data,selectedColumns,fraudCol);

#choose random cross validation data
[trainData,crossData]=internalChooseRandomData(data,crossFraudSize,crossNormalSize,fraudCol);
#choose random test validation data
[trainData,testData]=internalChooseRandomData(trainData,testFraudSize,testNormalSize,fraudCol);

#apply gauss to training data and also find best epsilon using cross data
[mu sigma epsilon]=trainAndFindBestEpsilon(trainData(:,selectedColumns),crossData(:,selectedColumns),crossData(:,fraudCol));

#apply gauss at test data
gaussFunctionResultOnTestData = multivariateGaussianFunction(testData(:,selectedColumns), mu, sigma);

#apply gauss at cross validation data, not needed , used only for to get total anomalies
gaussFunctionResultOnCrossData = multivariateGaussianFunction(crossData(:,selectedColumns), mu, sigma);

#find anomalies
flaggedFraudRowsOnTest=gaussFunctionResultOnTestData<epsilon;
flaggedFraudRowsOnCross=gaussFunctionResultOnCrossData<epsilon;


originalSize=size(data,1)
flaggedFraudAndAreFraudsOnTestSize=size(find((testData(flaggedFraudRowsOnTest,fraudCol)==1)),1);

flaggedTotalFraud=[testData(flaggedFraudRowsOnTest,:);crossData(flaggedFraudRowsOnCross,:)];

#remove frauds from data
testData(flaggedFraudRowsOnTest,:)=[];
crossData(flaggedFraudRowsOnCross,:)=[];

flaggedFraudButAreNotFrauds=flaggedTotalFraud(flaggedTotalFraud(:,fraudCol)==0,:);
notFoundFrauds=[testData(testData(:,fraudCol)==1,:);crossData(crossData(:,fraudCol)==1,:)];
regular=[trainData(trainData(:,fraudCol)==0,:);testData(testData(:,fraudCol)==0,:);crossData(crossData(:,fraudCol)==0,:)];

#Results display
flaggedFraudButAreNotFraudsSize=size(flaggedFraudButAreNotFrauds,1)

flaggedFraudAndAreFraudsOnTestSize
flaggedTotalFraudSize=size(flaggedTotalFraud,1)

flaggedFraudAndAreFrauds=flaggedTotalFraud(find(flaggedTotalFraud(:,fraudCol)==1),:);
foundFraudFromFlaggedSize=size(flaggedFraudAndAreFrauds,1)

notFoundFraudSizeFromTest=testFraudSize-flaggedFraudAndAreFraudsOnTestSize
totalNotFoundFraudSize=size(notFoundFrauds,1)

regularSize=size(regular,1)

allSize=flaggedTotalFraudSize+totalNotFoundFraudSize+regularSize
diff=allSize-size(data,1)

#Plot results
colorsIDs=zeros(allSize,1);
colors = [
     0 0 1   #blue
     1 0 1 #magenta
     1 0 0   #red
     0 1 0   #green
     ];
colorsIDs=colorsIDs+([ones(flaggedTotalFraudSize-foundFraudFromFlaggedSize,1);zeros(allSize-flaggedTotalFraudSize+foundFraudFromFlaggedSize,1)]);
colorsIDs=colorsIDs+([zeros(flaggedTotalFraudSize-foundFraudFromFlaggedSize,1);ones(totalNotFoundFraudSize,1)+1;zeros(allSize-flaggedTotalFraudSize+foundFraudFromFlaggedSize-totalNotFoundFraudSize,1)]);
colorsIDs=colorsIDs+([zeros(flaggedTotalFraudSize-foundFraudFromFlaggedSize,1);zeros(totalNotFoundFraudSize,1);(ones(regularSize,1)+2);zeros(allSize-flaggedTotalFraudSize+foundFraudFromFlaggedSize-totalNotFoundFraudSize-regularSize,1)]);
colorsIDs=colorsIDs+([zeros(flaggedTotalFraudSize-foundFraudFromFlaggedSize,1);zeros(totalNotFoundFraudSize,1);zeros(regularSize,1);(ones(foundFraudFromFlaggedSize,1)+3)]);

all=[flaggedFraudButAreNotFrauds;notFoundFrauds;regular;flaggedFraudAndAreFrauds];
figure(2)
all=all(:,selectedColumns);
[allNorm, mu, sigma] = internalNormalize(all);
[U, S] = pca(allNorm);
Z = internalProjectData(allNorm, U, 2);
scatter(Z(:,1), Z(:,2), [], colors(colorsIDs,:),'filled');

end