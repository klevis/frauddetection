function internalPlot(data,dataToPrint,isFraudColumn)

colors = [
     1 0 0   #red
     1 0 1 #magenta
     ];

colorIds=(data(:,isFraudColumn)==1)+1;
marksSize=((data(:,isFraudColumn)==1)+1)*17;

scatter(dataToPrint(:,1), dataToPrint(:,2), marksSize, colors(colorIds,:),'filled');

end;