function reg = regularization(thetasMatrices,lambda,m,biais)

    %% L2 regularization for neural network with lenthetasMatrices layers
    %% thetasMatrices is vector containing matrices e.g thetasMatrices{1} = ones(10,10)
    %% lambda a parameter used on the regularization formula
    %% m is the number elements on training set
    %% biais is a boolean: true means the first columns of each matrices is a one vector. First column will be ignored
                        %% false means first columns have already been removed
    %% The function should work for any size of vector thetasMatrices
    
    reg = 0;
    
    if size(thetasMatrices,1) ~= 1 && size(thetasMatrices,2)~= 1; %% If thetaMatrices is not a vector the function will throw an error and break
        error('ThetasMatrices is not a vector')
        return
    else
        
        if size(thetasMatrices,1) ~= 1;                           %% if number of rows is >1 then we transpose thetasMatrices to facilitate the next part
            thetasMatrices = thetasMatrices';                     %% of the code  
        end
        
        lenthetasMatrices = size(thetasMatrices,2);               %% length of thetasMatrices
        
        switch biais

            case true

                for i=1:lenthetasMatrices;                        %% loops through thetasMatrices and creates a vector of lenthetasMatrices size containing
                    reg(i) = sum(sum(thetasMatrices{i}(:,2:size(thetasMatrices{i},2)).^2)); %% the actual formula for regularization ignoring the first columns
                end

            case false

                for i=1:lenthetasMatrices;                        %% loops through thetasMatrices and creates a vector of lenthetasMatrices size containing
                    reg(i) = sum(sum(thetasMatrices{i}.^2));      %% the actual formula for regularization not ignoring the first columns
                end
        end
    end
    
    reg = (lambda / (2*m))*sum(reg);                              %% results weighting using lambda
    
end