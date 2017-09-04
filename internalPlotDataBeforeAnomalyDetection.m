function internalPlotDataBeforeAnomalyDetection(data,selectedColumns,fraudColumn)
figure(1);
[dataNorm, mu, sigma] = internalNormalize(data(:,selectedColumns));
[U, S] = pca(dataNorm);
Z = internalProjectData(dataNorm, U, 2);
internalPlot(data,Z,fraudColumn);
end