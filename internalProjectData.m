function Z = internalProjectData(X, U, K)
Z = zeros(size(X, 1), K);
U_reduce = U(:, 1:K);
Z=X*U_reduce;
end
