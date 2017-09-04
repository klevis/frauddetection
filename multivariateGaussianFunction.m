function p = multivariateGaussianFunction(data, mu, sigma)

k = length(mu);

if (size(sigma, 2) == 1) || (size(sigma, 1) == 1)
    sigma = diag(sigma);
end

data = bsxfun(@minus, data, mu(:)');
p = (2 * pi) ^ (- k / 2) * det(sigma) ^ (-0.5) * ...
    exp(-0.5 * sum(bsxfun(@times, data * pinv(sigma), data), 2));

end