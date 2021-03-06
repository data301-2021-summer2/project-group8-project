import pandas as pd
import numpy as np
from pandas_profiling import ProfileReport

# Filters the number of reviews into categories
def filterReviews(reviews): 
    if(reviews < 5):
        return "Less than 5"
    elif(reviews >= 5 and reviews < 10):
        return "Between 5 and 10"
    elif(reviews >= 10 and reviews < 20):
        return "Between 10 and 20"
    elif(reviews >= 20 and reviews < 35):
        return "Between 20 and 35"
    elif(reviews >= 35 and reviews < 50):
        return "Between 35 and 50"
    elif(reviews >= 50 and reviews < 75):
        return "Between 50 and 75"
    elif(reviews >= 75 and reviews < 100):
        return "Between 75 and 100"
    else: 
        return "More than 100"
    
def load_and_process(url_or_path_to_csv_file):

    # Method Chain 1 (Load data and deal with missing data)
    df1 = (
          pd.read_csv(url_or_path_to_csv_file)
          .loc[:, [
              "id",
                    "neighbourhood_cleansed",
                    "host_response_time",
                    "host_is_superhost",
                    "property_type",
                    "room_type",
                    "bathrooms_text",
                    "bedrooms",
                    "beds",
                    "amenities",
                    "price",
                    "availability_365",
                    "minimum_nights",
                    "number_of_reviews",
                    "review_scores_accuracy",
                    "review_scores_cleanliness",
                    "review_scores_communication",
                    "review_scores_value",
                    "reviews_per_month",
                    "calculated_host_listings_count",
                    "longitude",
                    "latitude"
          ]]
          .rename(columns={
                    "id": "Airbnb_Id",
                    "neighbourhood_cleansed": "Neighbourhood",
                    "host_response_time": "Host_ResponseTime",
                    "host_is_superhost": "Superhost",
                    "property_type": "Property_Type",
                    "room_type": "Room_Type",
                    "bathrooms_text": "Num_Baths_Detailed",
                    "bedrooms": "Num_Bedrooms",
                    "beds": "Num_Beds",
                    "price": "Price_per_Night",
                    "amenities": "Amenities",
                    "minimum_nights": "Minimum_Nights",
                    "availability_365": "Availablility_365Days",
                    "number_of_reviews": "Num_Reviews",
                    "review_scores_accuracy": "Accuray_ReviewScore",
                    "review_scores_cleanliness": "Cleanliness_ReviewScore",
                    "review_scores_communication": "Communication_ReviewScore",
                    "review_scores_value": "Value_ReviewScore",
                    "reviews_per_month": "Reviews_per_Month",
                    "calculated_host_listings_count": "Num_Host_Listings"
                    }, errors= "raise")
            .dropna()
      )
    
    
    # Method Chain 2 (Create new columns, drop others, and do processing)
    df2 = ( df1
           .assign(Superhost = df1["Superhost"].apply(lambda x: x == "t"))
           .assign(Num_Baths = df1["Num_Baths_Detailed"].apply(lambda x: (str(x)[0])))
           .assign(Price_per_Night = df1["Price_per_Night"].apply(lambda x: float(x[1:].replace(",",""))))
           .assign(Amenities = df1["Amenities"].apply(lambda x: len(x)))
           .assign(Num_Reviews = df1["Num_Reviews"].apply(lambda x: filterReviews(x)))
           .reset_index(drop=True)
          )
    
    ###Create 3 New Columns :"Minimum_Income_per_Booking","Yearly_Income", "Monthly_Income"###

    #Column "Minimum_Income_per_Booking" is calculated by multiplying "Price_per_Night" and "Minimum_Nights"

    df2['Minimum_Income_per_Booking'] = df2['Price_per_Night']*df2['Minimum_Nights']
    
    #Column "Monthly_Income" is calculated by multiplying "Minimum_Income_per_Booking" and "Reviews_per_Month"

    # ASSUMPTION: Guests may or may not leave reviews after their stay, however for our analysis purposes, 
    # we are assuming all guests leave reviews
    # ASSUMPTION: Yearly and Monthly income estimates are conservative and could be higher
    
    df2['Monthly_Income'] = df2['Minimum_Income_per_Booking']*df1['Reviews_per_Month']
    
    #Column "Yearly_Income" is calculated by multiplying "Monthly_Income" times "12

    df2['Yearly_Income'] = df2['Monthly_Income']*12


    # Shifts the columns around to the right position
    first_column = df2.pop('Num_Baths')
    df2.insert(0, 'Num_Baths', first_column)
    return df2.dropna()