import pandas as pd
import numpy as np
import geopy.distance


num_of_cows = 6

# This loop runs for every cow

for i in range(1, num_of_cows+1):
    current_cow = 'cow' + str(i)

    name_of_file = 'COW' + str(i) + ".csv"

    globals()[current_cow + "_data"] = pd.read_csv(name_of_file)
    # Holds list off the data dates of each cow
    globals()[current_cow + "_date_list"] = globals()[current_cow + "_data"]["Date"].tolist()
    # Holds tuple off the data dates of each cow
    globals()[current_cow + "_date_tuple"] = tuple(globals()[current_cow + "_date_list"])
    # The number of measurements of a cow per day
    globals()[current_cow + "_num_of_days"] = {}
    # Array with longitude and latitude of the measurements of the cows
    globals()[current_cow + "_la_lo_array"] = globals()[current_cow + "_data"].iloc[:, [2, 3]].to_numpy()

    # Sets values inside _num_of_days array
    for day in globals()[current_cow + "_date_tuple"]:
        globals()[current_cow + "_num_of_days"][str(day)] = 0

    for day in globals()[current_cow + "_date_list"]:
        globals()[current_cow + "_num_of_days"][str(day)] += 1

    # Dataframe that Holds  the relevant data
    globals()[current_cow + "_filter"] = pd.read_csv(name_of_file)
    # Change the columns type to a string
    globals()[current_cow + "_filter"]["Date"] = globals()[current_cow + "_filter"]["Date"].values.astype(str)
    globals()[current_cow + "_filter"][" Time"] = globals()[current_cow + "_filter"][" Time"].values.astype(str)
    # Create a new column and define its type
    globals()[current_cow + "_filter"]["Datetime"] = globals()[current_cow + "_filter"]["Date"] + globals()[current_cow + "_filter"][" Time"]
    globals()[current_cow + "_filter"]["Datetime"] = globals()[current_cow + "_filter"]["Datetime"].values.astype(str)
    # Indexing the column for filtering
    globals()[current_cow + "_filter"].set_index('Datetime', inplace=True)
    globals()[current_cow + "_filter"].index = pd.to_datetime(globals()[current_cow + "_filter"].index)
    # Filter by the hours appropriate to the question
    globals()[current_cow + "_filter"] = globals()[current_cow + "_filter"].between_time(start_time = '00:00:00', end_time= '5:00:00')
    # Removing unnecessary columns
    globals()[current_cow + "_filter"].drop([' PDOP', ' HDOP', ' TTF [s]', ' Satellites', ' Altitude', ' Info', 'Date', ' Time'], axis=1, inplace=True)

    # Returns the original index
    globals()[current_cow + "_filter"].reset_index(inplace=True)
    # Set up a new column that contains the distance of the cow since its last measurement
    globals()[current_cow + "_filter"]['Distance'] = 0
    # Add a column of a cow number
    globals()[current_cow + "_filter"]['Cow'] = i
    pre_cords = (0,0)

    # Enter information in a column that defines the distance of the cow
    for row in range(len(globals()[current_cow + "_filter"])):
        temp_longitude = globals()[current_cow + "_filter"].iloc[row][" Longitude"].item()
        temp_latitude = globals()[current_cow + "_filter"].iloc[row][" Latitude"].item()

        current_cords = (temp_longitude, temp_latitude)

        if row == 0:
            pre_cords = current_cords
            continue

        globals()[current_cow + "_filter"].at[row, 'Distance'] = geopy.distance.distance(current_cords, pre_cords).m

        pre_cords = current_cords
# Merge the cows dataframes
general = pd.concat([globals()["cow1_filter"], globals()["cow2_filter"], globals()["cow3_filter"],
                     globals()["cow4_filter"], globals()["cow5_filter"], globals()["cow6_filter"]])

print(general)
print(type(general))


