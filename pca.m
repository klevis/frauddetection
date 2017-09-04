function [U, S] = pca(data)

[m, n] = size(data);

U = zeros(n);
S = zeros(n);

sigma=(1/m)*data'*data;
[U, S, V] = svd(sigma);
end
