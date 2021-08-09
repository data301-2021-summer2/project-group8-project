import pandas as pd
import numpy as np
from pandas_profiling import ProfileReport

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
           .reset_index(drop=True)
          )

    # Shifts the columns around to the right position
    first_column = df2.pop('Num_Baths')
    df2.insert(0, 'Num_Baths', first_column)
    return df2