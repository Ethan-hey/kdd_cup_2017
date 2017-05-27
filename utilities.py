
# coding: utf-8

# In[1]:

import numpy as np


# In[1]:

def MAPE(preds, outputs):
    preds = np.array(preds)
    outputs = np.array(outputs)
    return np.average(np.abs(outputs - preds) / outputs)


# In[2]:

def date_converter(volume, weather):
    # Initialize lists to store splitted information
    date_list = []
    month_list = []
    day_list = []
    hour_list = []
    minute_list = []
    rounded_min_list = []
    rounded_hour_list = []

    # Splits the 'time' information into month, day, hour and minute
    def date_spliter(date):
        counter = 0
        parts = date.split(" ")
        day_part = parts[0]
        clock_part = parts[1]

        day_parts = day_part.split("-")
        month = int(day_parts[1]) # Extract month from time
        day = int(day_parts[2]) # Extract day from time

        clock_parts = clock_part.split(":")
        hour = int(clock_parts[0]) # Extract hour from time
        minute = int(clock_parts[1]) # Extract minute from time

        rounded_hour = str(hour // 3 * 3)
        rounded_min = str(minute // 20 * 20)

        date_list.append(day_part)
        month_list.append(month)
        day_list.append(day)
        hour_list.append(hour)
        minute_list.append(minute)
        rounded_hour_list.append(rounded_hour)
        rounded_min_list.append(rounded_min)

    # Store info into lists
    for date in volume['time']:
        date_spliter(date)

    # Add arrays into the 'volume' SFrame
    volume['month'] = np.array(month_list)
    volume['day'] = np.array(day_list)
    volume['hour'] = np.array(hour_list)
    volume['minute'] = np.array(minute_list)
    volume['date'] = np.array(date_list)
    volume['rounded_hour'] = np.array(rounded_hour_list)
    volume['rounded_min'] = np.array(rounded_min_list)
    
    # Add an colume which combine 'date' and 'rounded_hour'
    print type(volume['date'][0])
    slash_list = np.array(['-'] * len(volume['date']))
    volume['date_and_rounded_hour'] = volume['date'] + slash_list + volume['rounded_hour']
    
    slash_list = np.array(['-'] * len(weather['date']))
    weather['date_and_rounded_hour'] = weather['date'] + slash_list + np.array([str(hour) for hour in weather['hour']])
    
    return (volume, weather)


# In[3]:

def build_merged_tables(volume, weather, join_type='inner'):
    # Merge 'volume' and 'weather' DataFrame together
    volume_weather = pd.merge(volume, weather, on='date_and_rounded_hour', suffixes=('', '_y'))

    # Construct 'window_time' list which uses date, hour, and rounded minute
    date_list = volume_weather['date'] + np.array(['-'] * len(volume_weather))
    hour_list = volume_weather['hour'].astype(str) + np.array(['-'] * len(volume_weather))
    window_time_list = date_list + hour_list + volume_weather['rounded_min']
    volume_weather['window_time'] = window_time_list
    
    return volume_weather


# In[4]:

# Group 'volume_weather' here
def group_vw(volume_weather):
    
    volume_weather = volume_weather.groupby(['window_time', 'tollgate_id', 'direction'])
    vwgrouped = volume_weather.agg('mean').join(pd.DataFrame(volume_weather.size(), columns=['count']))

    # Put index as column
    vwgrouped['direction'] = vwgrouped.index.get_level_values('direction')
    vwgrouped['tollgate_id'] = vwgrouped.index.get_level_values('tollgate_id')
    vwgrouped['window_time'] = vwgrouped.index.get_level_values('window_time')
    
    return vwgrouped


# In[5]:

def re_construct_data(volume_weather):

    # Create 'weekday' column in DateFrame(0 stands for Sunday; 1 stands for Monday and 2 stands for Tuesday, etc...)
    sept = volume_weather[volume_weather['month'] == 9]
    weekday1 = ((sept['day'] + 3) % 7).values
    octo = volume_weather[volume_weather['month'] == 10]
    weekday2 = ((octo['day'] + 5) % 7).values
    volume_weather['weekday'] = np.append(weekday1, weekday2)
    
    # Create 'rounded_min' column
    volume_weather['rounded_min'] = volume_weather['minute'] // 20 * 20
    
    # Construc splitted numerical data column
    def split_value(df, column, values_list):
        for i in range(len(values_list) - 1):
            begin = values_list[i]
            end = values_list[i+1]
            new_column_name = column + "_" + str(begin) + "_" + str(end)
            df[new_column_name] = (df[column] >= begin).astype(int) & (df[column] < end).astype(int)
        return df
    
    # Create 'is_column_x' column in DataFrame
    def create_is_columns(df, column_names):
        for column_name in column_names:
            for i in np.sort(df[column_name].unique()):
                new_column_name = "is_" + column_name + "_" + str(i)
                df[new_column_name] = (df[column_name] == i).astype(int)
        return df

    is_columns = ['rounded_min', 'hour', 'tollgate_id', 'direction', 'weekday' ]
    create_is_columns(volume_weather, is_columns)

    # Construct 'is_festival' column
    volume_weather['is_festival'] = np.array([0] * len(volume_weather))
    sep_days = [15, 16, 17]
    oct_days = [1, 2, 3, 4, 5, 6, 7]
    for day in sep_days:
        volume_weather.loc[((volume_weather['month'] == 9) & (volume_weather['day'] == day)), 'is_festival'] = 1
    for day in oct_days:
        volume_weather.loc[((volume_weather['month'] == 10) & (volume_weather['day'] == day)), 'is_festival'] = 1
        
    # Construct 'is_working_day' column
    volume_weather['is_working_day'] = np.array([0] * len(volume_weather))
    volume_weather.loc[((volume_weather['weekday'] < 5) & (volume_weather['weekday'] > 0)), 'is_working_day'] = 1
    volume_weather.loc[volume_weather['is_festival'] == 1, 'is_working_day'] = 0
    volume_weather.loc[((volume_weather['month'] == 9) & (volume_weather['day'] == 18)), 'is_working_day'] = 1
    volume_weather.loc[((volume_weather['month'] == 10)
                        & ((volume_weather['day'] == 8) | (volume_weather['day'] == 9))), 'is_working_day'] = 1

    
    return volume_weather


# ## Show subplots

# In[6]:

# fig = plt.figure()
# ax1 = fig.add_subplot(211)
# ax2 = fig.add_subplot(212)

# ax1.plot(vwgrouped['pressure'], vwgrouped['count'], '.')
# print np.corrcoef(vwgrouped['pressure'], vwgrouped['count'])

# ax2.plot(nf_vwgroup['pressure'], nf_vwgroup['count'], '.')
# print np.corrcoef(nf_vwgroup['pressure'], nf_vwgroup['count'])

