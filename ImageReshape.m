function [scaled_image,reshaped_image] = ImageReshape(image,targetSize)
    % This function will resize a image based on the TargetSize variable
    % (which must be an integer).
    syms ratio
    image = image(:,1:size(image,1));
    eqn = ratio * size(image,1) == targetSize;
    ratio = double(solve(eqn,ratio));
    scaled_image = imresize(image,ratio); %Will print a scaled/resized image
    reshaped_image = reshape(scaled_image,1,targetSize^2); %Reshaped image 1 row and targetsize^2 columns
    
end