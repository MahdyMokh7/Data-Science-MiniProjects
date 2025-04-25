import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.dates as mdates

# Assuming you've already loaded the dataframes for historical and real-time data

# Convert the 'transaction_date' from the historical data to datetime
dfs_historical_dict['TransactionPatterns_daily_trend']['transaction_date'] = pd.to_datetime(dfs_historical_dict['TransactionPatterns_daily_trend']['transaction_date'])

# Convert the 'window.start' from real-time data to datetime
dfs_realtime_dict['darooghe.streamApp.minute']['window.start'] = pd.to_datetime(dfs_realtime_dict['darooghe.streamApp.minute']['window.start'])

# Plot the historical data
plt.figure(figsize=(12, 6))

# Historical data: daily transactions (TransactionPatterns_daily_trend)
plt.plot(dfs_historical_dict['TransactionPatterns_daily_trend']['transaction_date'],
         dfs_historical_dict['TransactionPatterns_daily_trend']['daily_transactions'],
         label='Historical Data', color='blue', linestyle='-', linewidth=2)

# Real-time data: minute-wise transactions (darooghe.streamApp.minute)
plt.plot(dfs_realtime_dict['darooghe.streamApp.minute']['window.start'],
         dfs_realtime_dict['darooghe.streamApp.minute']['txn_count'],
         label='Real-time Data', color='red', linestyle='-', linewidth=2)

# Add a vertical line where the real-time data starts (end of historical data)
historical_end_date = dfs_historical_dict['TransactionPatterns_daily_trend']['transaction_date'].max()
plt.axvline(historical_end_date, color='black', linestyle='--', label='Transition Point')

# Formatting the plot
plt.title('Transaction Volume: Historical vs. Real-time Data', fontsize=16)
plt.xlabel('Date', fontsize=12)
plt.ylabel('Transaction Volume', fontsize=12)
plt.xticks(rotation=45)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.gca().xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
plt.legend(loc='upper left')
plt.grid(True)

# Show the plot
plt.tight_layout()
plt.show()



#########################################################


import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.dates as mdates

# Assuming you've already loaded the dataframes for historical and real-time data

# Convert the 'transaction_date' from the historical data to datetime
dfs_historical_dict['TransactionPatterns_daily_trend']['transaction_date'] = pd.to_datetime(dfs_historical_dict['TransactionPatterns_daily_trend']['transaction_date'])

# Convert the 'window.start' from real-time data to datetime
dfs_realtime_dict['darooghe.streamApp.minute']['window.start'] = pd.to_datetime(dfs_realtime_dict['darooghe.streamApp.minute']['window.start'])

# Plot the historical data
plt.figure(figsize=(17, 6))

# Historical data: daily transactions (TransactionPatterns_daily_trend)
plt.plot(dfs_historical_dict['TransactionPatterns_daily_trend']['transaction_date'],
         dfs_historical_dict['TransactionPatterns_daily_trend']['daily_transactions'],
         label='Historical Data', color='blue', linestyle='-', linewidth=2)

# Real-time data: minute-wise transactions (darooghe.streamApp.minute)
plt.plot(dfs_realtime_dict['darooghe.streamApp.minute']['window.start'],
         dfs_realtime_dict['darooghe.streamApp.minute']['txn_count'],
         label='Real-time Data', color='orange', linestyle='-', linewidth=2)

# Add a vertical line where the real-time data starts (end of historical data)
historical_end_date = dfs_historical_dict['TransactionPatterns_daily_trend']['transaction_date'].max()
plt.axvline(historical_end_date, color='black', linestyle='--', label='Transition Point')

# Highlight the right part (real-time data)
plt.axvspan(historical_end_date, dfs_realtime_dict['darooghe.streamApp.minute']['window.start'].max(), color='orange', alpha=0.3)

# Formatting the plot
plt.title('Transaction Volume: Historical vs. Real-time Data', fontsize=16)
plt.xlabel('Date', fontsize=12)
plt.ylabel('Transaction Volume', fontsize=12)

# Change x-axis to show Month-Day, and add the year elsewhere in the plot
plt.xticks(rotation=45)
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))  # Show only Month-Day
plt.gca().xaxis.set_major_locator(mdates.MonthLocator())  # Adjust ticks to show every month (or can use other frequency)

# Add the year annotation once somewhere on the plot
plt.annotate('Year: 2025', xy=(0.5, 0.95), xycoords='axes fraction', ha='center', fontsize=10, color='black')

# Remove grid lines
plt.grid(False)

# Add legend
plt.legend(loc='upper left')

# Show the plot
plt.tight_layout()
plt.show()
