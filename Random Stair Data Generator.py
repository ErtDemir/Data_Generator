import pandas as pd
import random
import datetime


def calc_data_len(distance,
                  count_month):  # Calculate the data length based on distance between sequential 2 data and giving number of month
    count_of_distance = 60 // distance
    data_len = count_of_distance * 24 * 30 * count_month
    return data_len


def inc_time(time):  # time's type must be datetime  || Increase the time approximately 1 minute
    decider = random.randint(0, 2)
    if decider == 0:
        new_time = time + datetime.timedelta(seconds=(50 + random.randint(0, 5)))
    elif decider == 1:
        new_time = time + datetime.timedelta(minutes=1, seconds=random.randint(0,
                                                                               5))  # if concat two of them and separate them, reduce the possibilities of both of th
    else:  #
        new_time = time + datetime.timedelta(minutes=1, seconds=5 + random.randint(0, 5))  #
    return new_time


def change_ec(lower_bound):  # Decides how to intervene in ec based on time
    decider = random.randint(0, 1)
    if decider == 0:
        new_ec = lower_bound + random.randint(2, 5) * random.randint(4, 6)
        return new_ec
    new_ec = lower_bound - random.randint(2, 5) * random.randint(4, 6)
    return new_ec


def status_creat(decisive_number):  # Decides to what is next status randomly but possibility of abnormal is tiny
    decider = random.randint(1, 33333)
    if decider == decisive_number:
        return "abnormal"
    return "normal"


def abnormal_ec(current_ec):  # Generate abnormal ec based on current ec,increase (60 - 100) or decrease (60 - 100)
    coef = random.randint(50, 75)
    decider = random.randint(1, 2)
    print("Abnormal data allocated")
    if decider == 1:
        new_ec = current_ec + coef * 4
    else:
        new_ec = current_ec - coef * 4
    return new_ec


# The main new ec generate function. It uses upper method.
def change_append_ec(current_ec, df, lower_bound, current_time, colm, anomaly_time_list, decisive_number):
    new_time = inc_time(current_time)
    status = status_creat(decisive_number)

    if status == "normal":
        # change ec are going to call in this part
        new_ec = change_ec(lower_bound)
        new_df = pd.DataFrame(columns=colm)
        first_row_dict = {colm[0]: new_time, colm[1]: new_ec, colm[2]: status}
        new_df = new_df.append(first_row_dict, ignore_index=True)

    else:  # abnormal ec are going to call in this part
        new_ec = abnormal_ec(current_ec)
        new_df = pd.DataFrame(columns=colm)
        first_row_dict = {colm[0]: new_time, colm[1]: new_ec, colm[2]: status}
        new_df = new_df.append(first_row_dict, ignore_index=True)
        anomaly_time_list.append(new_time)

    df = df.append(new_df, ignore_index=True)
    return new_ec, new_time, df


def is_it_head_of_month(time):
    if time.day == 1 and time.hour == 10 and (time.minute == 15 or time.minute == 16):
        return True
    return False


# Declare basic values
lastData = 650
lowerBound = 650
decisiveNumber = 33334

# ---------------------
# create data length
distanceEachData = 1  # minutes
month = 5

dataLen = calc_data_len(distanceEachData, month)
# ---------------------

# Creates empty list going to use
listSyntheticData = []
anomalyTimeList = []
dateList = []

# Declare time values
year = 2020
month = 1
day = 1
hour = 0
minute = 0
second = 0

# Creates DATE HEADERS AND DATA
date = datetime.datetime(year, month, day, hour, minute, second)
headers = ["timestamp", "ec", "status"]
data = [date, lastData, "normal"]

# Creates DATA FRAME and initialize the current values
firstRow = {headers[0]: data[0], headers[1]: data[1], headers[2]: data[2]}
syntheticDataDf = pd.DataFrame(columns=headers)
syntheticDataDf = syntheticDataDf.append(firstRow, ignore_index=True)
currentDate = date
currentEc = lastData

# Checking DF is exist
print(syntheticDataDf)

lastMonth = currentDate.month
# Start the Data Generate Part
for i in range(dataLen):

    # Start stair step
    if lastMonth != currentDate.month:
        lowerBound += 300
        lastMonth = currentDate.month

    # Start anomalies
    if currentDate.month == 4 and currentDate.day == 15:
        decisiveNumber = 11111

    # Next Data
    currentEc, currentDate, syntheticDataDf = change_append_ec(currentEc, syntheticDataDf, lowerBound,
                                                               currentDate, headers, anomalyTimeList, decisiveNumber)

    # Period length of the experiment period
    # if is_it_head_of_month(currentDate):
    #    currentEc = 150

# Export the DATA FRAMES as csv file
DataPath = "C:/Users/Ertugrul Demir/Desktop/randomSyntheticStairData.csv"
syntheticDataDf = syntheticDataDf.drop(columns="status")
syntheticDataDf.to_csv(DataPath, index=False, header=True)

# Create a DATA FRAME of abnormal date and export it as csv file
anoData = {"timestamp": anomalyTimeList}
AnoDatePath = "C:/Users/Ertugrul Demir/Desktop/AnoDateRandomStair.csv"
anoDateDf = pd.DataFrame(anomalyTimeList)
anoDateDf.to_csv(AnoDatePath, index=False, header=False)
