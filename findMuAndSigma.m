function [mu sigma] = findMuAndSigma(X)

[m, n] = size(X);
mu = zeros(n, 1);
sigma = zeros(n, 1);
mu=(1/m)*sum(X);
sigma=(1/(m))*sum(((X-mu).^2));
end
