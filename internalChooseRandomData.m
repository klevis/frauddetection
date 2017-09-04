function [newData,dataVal]=internalPickRandomFromData(data,randomFraudSize,randomNormalSize,fraudCol)

#filter randomFraudSize and non randomFraudSize
fraudRows=find(data(:,fraudCol)==1);
nonFraudRows=find(data(:,fraudCol)==0);

#choose random row index from randomFraudSize and non randomFraudSize
randomFraudRows=fraudRows(randperm(size(fraudRows, 1),randomFraudSize));
randomNonFraudRows=nonFraudRows(randperm(size(nonFraudRows, 1),randomNormalSize));

#build cross validation matrix data
dataVal=[data(randomFraudRows,:);data(randomNonFraudRows,:)];

#remove cross valiadtion data from original data
newData=data;
toBeRemoved=([randomFraudRows;randomNonFraudRows]);
newData(sort(toBeRemoved,"descend"),:)=[];
end