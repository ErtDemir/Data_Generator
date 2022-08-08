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


def get_part_day(time):  # Get the part of day based on current time's hour
    current_hour = time.hour
    if current_hour >= 21 or current_hour < 4:
        return "night"
    elif 4 <= current_hour <= 12:
        return "morning"
    elif 12 < current_hour < 21:
        return "afternoon"


def dec_ec(current_ec):  # Decrease ec (0.5 - 0.2)
    scale = 1 / random.randint(2, 5)
    new_ec = current_ec - scale
    return new_ec


def inc_ec(current_ec):  # Increase ec (0.04 - 0.02)
    scale = 1 / random.randint(25, 50)
    new_ec = current_ec + scale
    return new_ec


def sta_ec(current_ec):  # Stabilize ec (%% 1 up or down)
    if random.randint(0, 1) == 0:
        new_ec = current_ec + (current_ec / 10000)
    else:
        new_ec = current_ec - (current_ec / 10000)
    return new_ec


def change_ec(current_ec, part_of_day):  # Decides how to intervene in ec based on time
    if part_of_day == "night":
        new_ec = inc_ec(current_ec)
    elif part_of_day == "afternoon":
        new_ec = dec_ec(current_ec)
    else:
        new_ec = sta_ec(current_ec)
    return new_ec


def add_ec(current_ec, part_of_day):  # If ec less than lower threshold so run this function
    chosen_possibility = random.randint(0, 2)
    addition_scale = random.randint(10, 12)
    if chosen_possibility == 0 or chosen_possibility == 1:
        num_scale = random.randint(1, 2)
        new_ec = current_ec + (addition_scale * num_scale)
        second_new_ec = change_ec(new_ec, part_of_day)

    else:
        new_ec = current_ec + addition_scale
        second_new_ec = new_ec + addition_scale

    return [new_ec, second_new_ec]


def status_creat(decisive_number):  # Decides to what is next status randomly but possibility of abnormal is tiny
    decider = random.randint(1, 33333)
    if decider == decisive_number:
        return "abnormal"
    return "normal"


def abnormal_ec(current_ec):  # Generate abnormal ec based on current ec,increase (60 - 100) or decrease (60 - 100)
    coef = random.randint(50, 75)
    decider = random.randint(1, 2)
    print("Abnormal data allocated")
    new_ec = 0
    if decider == 1:
        new_ec = current_ec + coef * 4
    elif decider == 2:
        new_ec = current_ec - coef * 4
    return new_ec


# The main new ec generate function. It uses upper method.
def change_append_ec(current_ec, part_of_day, df, lower_bound, current_time, colm, anomaly_time_list, decisive_number):
    new_time = inc_time(current_time)
    status = status_creat(decisive_number)

    if status == "normal":

        if current_ec < lower_bound:  # Adding ec some case like Double adding. ex: 1149 -> 1162 -> 1173 but mostly it does one adding process
            new_ec_list = add_ec(current_ec, part_of_day)
            new_ec, second_new_ec = new_ec_list[0], new_ec_list[1]
            second_new_time = inc_time(new_time)

            new_df = pd.DataFrame(columns=colm)
            first_row_dict = {colm[0]: new_time, colm[1]: new_ec, colm[2]: status, colm[3]: lower_bound}
            second_row_dict = {colm[0]: second_new_time, colm[1]: second_new_ec, colm[2]: "normal", colm[3]: lower_bound}
            new_df = new_df.append(first_row_dict, ignore_index=True)
            new_df = new_df.append(second_row_dict, ignore_index=True)
            new_ec = second_new_ec
            new_time = second_new_time

        else:  # change ec are going to call in this part
            new_ec = change_ec(current_ec, part_of_day)
            new_df = pd.DataFrame(columns=colm)
            first_row_dict = {colm[0]: new_time, colm[1]: new_ec, colm[2]: status, colm[3]: lower_bound}
            new_df = new_df.append(first_row_dict, ignore_index=True)

    else:  # abnormal ec are going to call in this part
        new_ec = abnormal_ec(current_ec)
        new_df = pd.DataFrame(columns=colm)
        first_row_dict = {colm[0]: new_time, colm[1]: new_ec, colm[2]: status, colm[3]: lower_bound}
        new_df = new_df.append(first_row_dict, ignore_index=True)
        anomaly_time_list.append(new_time)

    df = df.append(new_df, ignore_index=True)
    return new_ec, new_time, df


def is_it_head_of_month(time):
    if time.day == 1 and time.hour == 10 and (time.minute == 15 or time.minute == 16):
        return True
    return False


# Declare basic values
lastData = 0
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
headers = ["timestamp", "ec", "status", "setpoint"]
data = [date, lastData, "normal", 0]

# Creates DATA FRAME and initialize the current values
firstRow = {headers[0]: data[0], headers[1]: data[1], headers[2]: data[2], headers[3]: data[3]}
syntheticDataDf = pd.DataFrame(columns=headers)
syntheticDataDf = syntheticDataDf.append(firstRow, ignore_index=True)
currentDate = date
currentEc = lastData

# Checking DF is exist
print(syntheticDataDf)

lastMonth = currentDate.month
# Start the Data Generate Part
for i in range(dataLen):

    # if currentDate.month > 4:
    #     lowerBound = 1650
    # elif currentDate.month > 3:
    #     lowerBound = 1450
    # elif currentDate.month > 2:
    #     lowerBound = 1150
    # elif currentDate.month > 1:
    #     lowerBound = 850

    # The good way
    if lastMonth != currentDate.month:
        lowerBound += 300
        lastMonth = currentDate.month
        print("Month is ", lastMonth)

    if currentDate.month == 4 and currentDate.day == 15:
        decisiveNumber = 11111

    partOfDay = get_part_day(currentDate)
    currentEc, currentDate, syntheticDataDf = change_append_ec(currentEc, partOfDay, syntheticDataDf, lowerBound,
                                                               currentDate, headers, anomalyTimeList, decisiveNumber)

    # if is_it_head_of_month(currentDate):  # Period length of the experiment period
    #    currentEc = 150

# Export the DATA FRAMES as csv file
DataPath = "C:/Users/Ertugrul Demir/Desktop/syntheticStairData.csv"
# syntheticDataDf = syntheticDataDf.drop(columns="status")
syntheticDataDf.to_csv(DataPath, index=False, header=True)

# Create a DATA FRAME of abnormal date and export it as csv file
anoData = {"timestamp": anomalyTimeList}
AnoDatePath = "C:/Users/Ertugrul Demir/Desktop/AnoDateStair.csv"
anoDateDf = pd.DataFrame(anomalyTimeList)
anoDateDf.to_csv(AnoDatePath, index=False, header=False)
