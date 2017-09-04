function [dataNorm, mu, sigma] = internalNormalize(data)

mu = mean(data);
dataNorm = bsxfun(@minus, data, mu);

sigma = std(dataNorm);
dataNorm = bsxfun(@rdivide, dataNorm, sigma);


% ============================================================

end
