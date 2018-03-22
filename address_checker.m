function [results] = address_checker(city_name,key)
    %this function will return a n*m matrix with the city name,region,
    %country, latitude and longitude if there is a match on google maps.
    %it uses Google maps API and therefor requires a valid key.
    
    if ~exist('key','var') % if no Google API key returned default to some key (a valid one)
        key = 'AIzaSyCPUQv3_nH-K55cGiSQhUXh3YW6EKYWSPY';
    end

    for x=1:size(city_name,1)
        
        if size(city_name,1) == 1;                                    %Replaces spaces by +
            varCity = replace(city_name,' ','+');
        else
            varCity = replace(city_name{x},' ','+');
        end
        
        [GoogleAPI_connection,status] = urlread(strcat('https://maps.googleapis.com/maps/api/geocode/json?address=',char(varCity),'&key=',key)); %uses Google API key and city to create and read a link
                                                                                                                                                 %the link return a json file including all sorts of information about the city looked up
        if status == 0;         %status equal to zero means the link created was incorrect therefore results array is set to NaN
            for i=1:5;
                results{x,i} = NaN;
            end
            continue
        else
            
        GoogleAPI_data = jsondecode(GoogleAPI_connection); %creates a structure using the json file returned by the API
      
        if string(GoogleAPI_data.status) == 'ZERO_RESULTS' || string(GoogleAPI_data.status) == 'INVALID_REQUEST'; %sets the results to NaN if no results or invalid requests
            for i=1:5;
                results{x,i} = NaN;
            end
        else
            
            if size(GoogleAPI_data.results,1) == 1; %if the json file includes more than 1 match then the results will be NaN otherwire, the utility will loop through the json file to get the requested information
                for i=1:size(GoogleAPI_data.results.address_components,1);

                    if string(GoogleAPI_data.results.address_components(i).types(1)) == 'locality'; %get city
                        results{x,1} = GoogleAPI_data.results.address_components(i).long_name;
                    elseif string(GoogleAPI_data.results.address_components(i).types(1)) == 'administrative_area_level_1'; %get region
                        results{x,2} = GoogleAPI_data.results.address_components(i).long_name;
                    elseif string(GoogleAPI_data.results.address_components(i).types(1)) == 'country'; %get country
                        results{x,3} = GoogleAPI_data.results.address_components(i).long_name;
                    end

                end;

                    results{x,4} = GoogleAPI_data.results.geometry.location.lat; %get latitude and longitude
                    results{x,5} = GoogleAPI_data.results.geometry.location.lng;
            else
                
                    for i=1:5;
                        results{x,i} = NaN;
                    end
            end
        end

     end

end
