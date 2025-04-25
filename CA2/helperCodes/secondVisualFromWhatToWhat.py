import matplotlib.pyplot as plt
import pandas as pd

# Assuming you've already loaded the dataframes for historical and real-time data

# Extract the merchant behavior data
merchant_behavior_df = dfs_historical_dict['TransactionPatterns_merchant_behavior']

# Sort by transaction count and get the top 5 merchants
top_5_merchants = merchant_behavior_df[['merchant_category', 'transaction_count']].sort_values(by='transaction_count', ascending=False).head(5)

# Plot the bar chart
plt.figure(figsize=(10, 6))

# Create the bar chart for the top 5 merchants
plt.bar(top_5_merchants['merchant_category'], top_5_merchants['transaction_count'], color='brown')

# Add labels and title
plt.title('Top 5 Merchants by Number of Transactions', fontsize=16)
plt.xlabel('Merchant Category', fontsize=12)
plt.ylabel('Number of Transactions', fontsize=12)

# Add the merchant names below the bars
plt.xticks(rotation=45, ha='right')

# Show the plot
plt.tight_layout()
plt.show()



##############################


import matplotlib.pyplot as plt
import pandas as pd

# Assuming you've already loaded the dataframes for historical and real-time data

# Extract the merchant behavior data
merchant_behavior_df = dfs_historical_dict['TransactionPatterns_merchant_behavior']

# Sort by transaction count and get the top 5 merchants
top_5_merchants = merchant_behavior_df[['merchant_category', 'transaction_count']].sort_values(by='transaction_count', ascending=False).head(5)

# Plot the horizontal bar chart
plt.figure(figsize=(10, 6))

# Create the horizontal bar chart for the top 5 merchants
plt.barh(top_5_merchants['merchant_category'], top_5_merchants['transaction_count'], color='brown')

# Add labels and title
plt.title('Top 5 Merchants by Number of Transactions', fontsize=16)
plt.xlabel('Number of Transactions', fontsize=12)
plt.ylabel('Merchant Category', fontsize=12)

# Add the merchant names on the y-axis
plt.tight_layout()
plt.show()


###################################

import matplotlib.pyplot as plt
import pandas as pd

# Assuming you've already loaded the dataframes for historical and real-time data

# Extract the merchant behavior data
merchant_behavior_df = dfs_historical_dict['TransactionPatterns_merchant_behavior']

# Sort by transaction count in descending order and get the top 5 merchants
top_5_merchants = merchant_behavior_df[['merchant_category', 'transaction_count']].sort_values(by='transaction_count', ascending=False).head(5)

# Plot the horizontal bar chart
plt.figure(figsize=(10, 6))

# Create the horizontal bar chart for the top 5 merchants
bars = plt.barh(top_5_merchants['merchant_category'], top_5_merchants['transaction_count'], color='#8B2513')  # More brown color

# Add labels and title
plt.title('Top 5 Merchants by Number of Transactions', fontsize=16)
plt.xlabel('Number of Transactions', fontsize=12)
plt.ylabel('Merchant Category', fontsize=12)

# Invert the y-axis to display the highest transactions on top
plt.gca().invert_yaxis()

# Change color of the y-tick labels: green for the max and red for the min
for i, label in enumerate(plt.gca().get_yticklabels()):
    if top_5_merchants['transaction_count'].iloc[i] == top_5_merchants['transaction_count'].max():
        label.set_color('green')  # Green for max transaction count
    elif top_5_merchants['transaction_count'].iloc[i] == top_5_merchants['transaction_count'].min():
        label.set_color('red')  # Red for min transaction count

# Adjust layout for tight fit
plt.tight_layout()

# Show the plot
plt.show()
