function [bestEpsilon bestF1] = selectThreshold(dataCross,crossFraudResults, crossFunctionResults)

bestEpsilon = 0;
bestF1 = 0;
F1 = 0;

stepsize = (max(crossFunctionResults) - min(crossFunctionResults)) / 1000;
for epsilon = min(crossFunctionResults):stepsize:max(crossFunctionResults)

    tp=sum((crossFraudResults == 1) & (crossFunctionResults <=epsilon));
    fp=sum((crossFraudResults == 0) & (crossFunctionResults <=epsilon));
    fn=sum((crossFraudResults == 1) & (crossFunctionResults > epsilon));

    prec =tp/(tp + fp);
    rec = tp/(tp + fn);
    F1 =2 * prec * rec/(prec + rec);
    if F1 > bestF1
       bestF1 = F1;
       bestEpsilon = epsilon;
    end
end

end
