function [J grad] = nnCostFunction(nn_params, ...
                                   input_layer_size, ...
                                   hidden_layer_size, ...
                                   num_labels, ...
                                   X, y, lambda)
%NNCOSTFUNCTION Implements the neural network cost function for a two layer
%neural network which performs classification
%   [J grad] = NNCOSTFUNCTON(nn_params, hidden_layer_size, num_labels, ...
%   X, y, lambda) computes the cost and gradient of the neural network. The
%   parameters for the neural network are "unrolled" into the vector
%   nn_params and need to be converted back into the weight matrices. 
% 
%   The returned parameter grad should be a "unrolled" vector of the
%   partial derivatives of the neural network.
%

% Reshape nn_params back into the parameters Theta1 and Theta2, the weight matrices
% for our 2 layer neural network
Theta1 = reshape(nn_params(1:hidden_layer_size * (input_layer_size + 1)), ...
                 hidden_layer_size, (input_layer_size + 1));

Theta2 = reshape(nn_params((1 + (hidden_layer_size * (input_layer_size + 1))):end), ...
                 num_labels, (hidden_layer_size + 1));

             size(Theta1)
% Setup some useful variables
m = size(X, 1);
  size(Theta2)       
% You need to return the following variables correctly 
J = 0;
Theta1_grad = zeros(size(Theta1));
Theta2_grad = zeros(size(Theta2));

%
% Part I - Forward propagation
X = [ones(m,1),X];
h0 = sigmoid([ones(m,1),sigmoid([X]*Theta1')]*Theta2'); %one line implementation;

a{1} = X*Theta1'; % a2
z{1} = sigmoid(a{1}); %z2
a{2} = [ones(m,1),z{1}]*Theta2'; %a3
z{2} = sigmoid(a{2});%z3

yVec = zeros(num_labels,m);

for i=1:num_labels
    yVec(i,:) = (y==i);
end

Thetas{1} = Theta1;
Thetas{2} = Theta2;

reg = regularization(Thetas,lambda,m,true);

J = (sum(sum(-yVec' .* log(h0) - (1- yVec)' .* (log(1-h0)))) / m) + reg ;

% Part 2 Backpropagation Vectorization
nLayers = 2;

for i=nLayers:-1:1
    if i == nLayers
        delta{i} = z{i} - yVec';
    else
        delta{i} = (Thetas{i+1}(:,2:end)' * delta{i+1}')' .* sigmoidGradient(a{i});
    end
end
% =========================================================================

Theta2_grad = Theta2_grad + (delta{2}' * [ones(m,1),z{1}]);
Theta1_grad = Theta1_grad + (delta{1}' * X);

Theta2_grad(:,1) = Theta2_grad(:,1) ./m;
Theta1_grad(:,1) = Theta1_grad(:,1) ./m;
Theta2_grad(:,2:end) = Theta2_grad(:,2:end) ./m + ( (lambda/m) * Thetas{2}(:,2:end) );
Theta1_grad(:,2:end) = Theta1_grad(:,2:end) ./m + ( (lambda/m) * Thetas{1}(:,2:end) );

% Unroll gradients
grad = [Theta1_grad(:) ; Theta2_grad(:)];


end
