function internalPCAAndPlot(data,useNormalization)

#skiping first row as it is the header
data=data(2:size(data,1),:);

isFraudColumn=9;

#skiping columns 9,10(fraud or not)
dataWithSelectedColumns=data(:,[1,2,3,4,5,6,7,8]);

if(useNormalization==true)
[dataNorm, mu, sigma] = internalNormalize(dataWithSelectedColumns);
else
dataNorm=dataWithSelectedColumns;
endif

[U, S] = pca(dataNorm);
Z = internalProjectData(dataNorm, U, 2);

internalPlot(data,Z,isFraudColumn);
end