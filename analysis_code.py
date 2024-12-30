import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os



# Read the Parquet files
orders_df = pd.read_parquet('orders.parquet')
errands_df = pd.read_parquet('errands.parquet')

# Capitalize the first letter of all column names
orders_df.columns = [col.capitalize() for col in orders_df.columns]
errands_df.columns = [col.capitalize() for col in errands_df.columns]

# Decode 'Order_number' from base-36 to base-10
def decode_base36(value):
    try:
        return int(value, 36)  # Convert base-36 to base-10
    except ValueError:
        return None  # Handle invalid values

errands_df["Order_id"] = errands_df["Order_number"].apply(decode_base36)

# Convert 'Order_id' to string and strip any leading/trailing spaces
orders_df['Order_id'] = orders_df['Order_id'].astype(str).str.strip()
errands_df['Order_id'] = errands_df['Order_id'].astype(str).str.strip()

# Merge the DataFrames on 'Order_id'
merged_df = pd.merge(errands_df, orders_df, on="Order_id", how="inner")

# Calculate the number of orders without contacting customer service
no_contact_orders = len(orders_df) - len(merged_df)

# Calculate the average number of customer contacts per order
percentage_with_contacts = (len(merged_df) / len(orders_df)) * 100 if len(orders_df) > 0 else 0

# Print  the average number of customer contacts per order
#print(f"Percentage of customer contacts per order: {percentage_with_contacts:.2f}%")

# Count the occurrences of each Order_id
order_counts = errands_df['Order_id'].value_counts()

# Filter to get only those Order_ids that appear once
unique_orders = order_counts[order_counts == 1]

# Number of those Order_ids that appear once
num_unique_orders = len(unique_orders)

# Percentage of orders with 1 contact among all contacts
percentage_unique_contact = (num_unique_orders / len(merged_df)) * 100

#print percentage of orders with 1 contact among all contacts
#print(f"Percentage of orders with 1 contact: {percentage_unique_contact:.2f}%")

# Filter to get only those Order_ids that appear twice
twice_orders = order_counts[order_counts == 2]
# Number of those Order_ids that appear twice
num_twice_orders = len(twice_orders)

# Calculate the average of customer contacts twice per order
percentage_two_contacts = (num_twice_orders / len(merged_df)) * 100

# Print the average of customer contacts twice per order
#print(f"Percentage of twice customer contacts per order: {percentage_two_contacts:.2f}%")

# Calculate the rate of customer contacts more than once per order
percentage_more_than_two_contacts = ((len(merged_df) - num_unique_orders) / len(merged_df)) * 100

#Print the rate of customer contacts more than once per order
#print(f"Percentage of customer contacts more than once per order: {percentage_more_than_two_contacts:.2f}%")

# Calculate the rate of orders without contacting customer service
percentage_no_contacts = (no_contact_orders / len(orders_df)) * 100

#Print the rate of orders without contacting customer service
#print(f"Percentage of orders without contacting customer service: {percentage_no_contacts:.2f}%")

#  Group by 'order_id' and count the number of rows per order
contacts_per_order = merged_df.groupby('Order_id').size()

# Calculate the frequency of each count
distribution = contacts_per_order.value_counts().sort_index()

# Print the average number of orders without contacting customer service
#print(f"Percentage of orders without contacting customer service: {percentage_no_contacts:.2f}%")


# Save key metrics to a file
metrics = {
    "Percentage of Orders With Contacts": f"{percentage_with_contacts:.2f}%",
    "Percentage of Orders Without Contacts": f"{percentage_no_contacts:.2f}%",
    "Percentage of Orders With 1 Contact": f"{percentage_unique_contact:.2f}%",
    "Percentage of Orders With 2 Contacts": f"{percentage_two_contacts:.2f}%",
    "Percentage of Orders With >1 Contact": f"{percentage_more_than_two_contacts:.2f}%"
}

with open("key_metrics.txt", "w") as file:
    for key, value in metrics.items():
        file.write(f"{key}: {value}\n")
#print("Key metrics saved to 'key_metrics.txt'")






#Bar Chart function
def plot_bar_chart(data, title, xlabel, ylabel, color="blue", rotation=45, save_path=None, save=True):
    """
    Function to plot a bar chart with customizable color and x-axis label rotation.

    Parameters:
        data (pd.Series or pd.DataFrame): The data to plot. Use a Series for simple bar charts.
        title (str): Title of the chart.
        xlabel (str): Label for the x-axis.
        ylabel (str): Label for the y-axis.
        color (str or list): Color of the bars. Can be a single color or a list of colors.
        rotation (int or float): Rotation angle for the x-axis labels.
    """
    plt.figure(figsize=(10, 6))
    data.plot(kind="bar", color=color)  # Dynamically change color
    plt.title(title)
    plt.xlabel(xlabel, ha='center')
    plt.ylabel(ylabel)
    plt.xticks(rotation=rotation)  # Dynamically change rotation
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.tight_layout()

    # Save the plot with the title as the filename
    if save:
        save_path = os.path.join(os.path.dirname(__file__), f"{title}.png")
        #plt.savefig(save_path, format='png', dpi=300)
        #print(f"Plot saved to {save_path}")
    #plt.show()
    plt.close()



#  Group by 'order_id' and count the number of rows per order
contacts_per_order = merged_df.groupby('Order_id').size()  # Counts rows per 'order_id'
#  Calculate the frequency of each count
distribution = contacts_per_order.value_counts().sort_index()
#Print
#print(f"Distribution of Contacts per Order: {distribution.head(10)}")

#  Plot the distribution as a bar chart
plot_bar_chart(
    data=distribution.head(23),
    title="Distribution of Contacts per Order vs Number of Orders",
    xlabel="Number of Contacts per Order",
    ylabel="Number of Orders"
)



# Visualizations
# Pie chart for contact vs. no contact
labels = ["No Contact", "With Contact"]
sizes = [percentage_no_contacts, percentage_with_contacts]
colors = ["green", "blue"]

# Create the pie chart
plt.figure(figsize=(8, 8))
plt.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=140, colors=colors)
plt.title("Percentage of Orders With and Without Customer Service Contacts")

# Save the plot as an image file
#plt.savefig('Percentage_of_Orders_With_and_Without_Contacts.png')

# Display the plot
#plt.show()
plt.close()  # Close this figure if it's not needed
# Bar chart for contact distribution
contact_distribution = pd.Series({
    "1 Contact": percentage_unique_contact,
    "2 Contacts": percentage_two_contacts,
    ">1 Contact": percentage_more_than_two_contacts
})

#Print
#print(f"contact_distribution:{contact_distribution}")
# Create the bar chart
plot_bar_chart(
    data=contact_distribution,
    title="Distribution of Contacts per Order vs Percentage of Orders",
    xlabel="Contact Type",
    ylabel="Percentage of Orders",
    color="orange",
    rotation=0
)





# The frequency of errand categories
category_counts = errands_df["Errand_category"].value_counts().head(10)

# The bar chart for the frequency of Errand Categories
plot_bar_chart(
    data=category_counts,
    title="Top 10 Frequency of Errand Categories",
    xlabel="Errand Category",
    ylabel="Frequency",
    color="orange",
    rotation=45
)





# Analyze the distribution of communication channels
channel_counts = errands_df["Errand_channel"].value_counts()

# Visualize channel distribution
plt.figure(figsize=(8, 8))
channel_counts.plot(kind="pie", autopct="%1.1f%%", colors=sns.color_palette("pastel"))
plt.title("Distribution of Communication Channels")
plt.ylabel("")  # Remove the y-label for better appearance

# Save the plot as an image file
#plt.savefig('Distribution_of_Communication_Channels.png')

# Display the plot
#plt.show()
plt.close()  # Close this figure if it's not needed


# Count the frequency of each errand category
action_category = errands_df['Errand_category'].value_counts().head(10)

# Plot the distribution of errand actions
plot_bar_chart(
    data=action_category,
    title="Top 10 Errand category",
    xlabel="Errand category",
    ylabel="Frequency",
    color="teal",
)




# Count the frequency of each errand type
action_types = errands_df['Errand_type'].value_counts().head(10)

# Plot the distribution of errand types
plot_bar_chart(
    data=action_types,
    title="Top 10 Errand types",
    xlabel="Errand Type",
    ylabel="Frequency",
    color="green",
)




# Count the frequency of each errand action
action_counts = errands_df['Errand_action'].value_counts().head(10)

# Plot the distribution of errand actions

plot_bar_chart(
    data=action_counts,
    title="Top 10 Errand Actions",
    xlabel="Errand Action",
    ylabel="Frequency",
    color="blue",
)



# Compare customer contacts by brands

brand_counts = merged_df["Brand"].value_counts() # Count the frequency of each Brand

# Plot the distribution of Brand
plot_bar_chart(
    data=brand_counts ,
    title="Customer Contacts by Brand",
    xlabel="Brand",
    ylabel="Frequency",
    color="purple",
)


# Filter data for Brand A
brand_a_data = merged_df[merged_df['Brand'] == 'Brand A']

# Count occurrences of each errand category for Brand A
errand_category_counts = brand_a_data['Errand_category'].value_counts()



# Visualize errand categories for Brand A

plot_bar_chart(
    data=errand_category_counts.head(5) ,
    title="Top 5 Errand Categories for Brand A",
    xlabel="Errand Category",
    ylabel="Number of Contacts",
    color="blue",
    rotation=15
)






# Scatter plot: Order Amount vs. Number of Contacts
#merged_df["contact_count"] = merged_df.groupby("Order_id")["Order_id"].transform("size")
#sns.scatterplot(data=merged_df, x="Order_amount", y="contact_count", alpha=0.6, color="purple")
#plt.title("Order Amount vs. Customer Contacts")
#plt.xlabel("Order Amount")
#plt.ylabel("Number of Contacts")
#plt.grid()
#Save the plot as an image file
#plt.savefig('Order Amount vs. Customer Contacts.png')

# Show the plot
#plt.show()
plt.close()  # Close this figure if it's not needed

# Analyze orders without contacts by country
# Find orders without customer contacts
unmatched_orders = orders_df[~orders_df["Order_id"].isin(merged_df["Order_id"])]

# Count the number of orders without contacts by country
country_counts_no_contact = unmatched_orders["Site_country"].value_counts()
#print("No contacts by Country:\n", country_counts_no_contact)

# Visualization
plot_bar_chart(
    data=country_counts_no_contact.head(10),
    title="Top 10 Countries with No Customer Contacts",
    xlabel="Country",
    ylabel="Number of Contacts",
    color="teal",
    rotation=45
)

# Analyze contacts by country
country_contacts = merged_df["Site_country"].value_counts()
#print("Contacts by Country:\n", country_contacts)

# Visualization
plot_bar_chart(
    data=country_contacts.head(10),
    title="Top 10 Countries with Most Customer Contacts",
    xlabel="Country",
    ylabel="Number of Contacts",
    color="blue",
    rotation=45
)


# Ensure 'Order_created_at' is in datetime format
if not pd.api.types.is_datetime64_any_dtype(merged_df['Order_created_at']):
    merged_df['Order_created_at'] = pd.to_datetime(merged_df['Order_created_at'], errors='coerce')

# Drop rows where 'Order_created_at' could not be converted to datetime
merged_df = merged_df.dropna(subset=['Order_created_at'])

# Extract month from 'Order_created_at' and create a new column 'month' in both DataFrames
merged_df['month'] = merged_df['Order_created_at'].dt.month
if 'Order_created_at' in orders_df.columns:
    orders_df['Order_created_at'] = pd.to_datetime(orders_df['Order_created_at'], errors='coerce')
    orders_df = orders_df.dropna(subset=['Order_created_at'])
    orders_df['month'] = orders_df['Order_created_at'].dt.month

# Analyze seasonal trends
monthly_orders = orders_df.groupby('month').size()
monthly_contacts = merged_df.groupby('month').size()
monthly_contact_rate = (monthly_contacts / monthly_orders).fillna(0) * 100

# Visualize monthly trends
plot_bar_chart(
    data=monthly_contact_rate.head(10),
    title="Customer Service Contact Rate by Month",
    xlabel="Month",
    ylabel="Contact Rate (%)",
    color="purple"
)

# Deduplicate orders in merged_df
merged_df_partner = merged_df.drop_duplicates(subset=["Order_id", "Partner"])

# Count total unique orders and contacts by partner
total_orders_by_partner = orders_df.groupby("Partner")["Order_id"].nunique()
contacts_by_partner = merged_df_partner.groupby("Partner")["Order_id"].nunique()

# Calculate interaction rates by partner
interaction_rate_by_partner = (contacts_by_partner / total_orders_by_partner).fillna(0) * 100

# Sort values
interaction_rate_by_partner = interaction_rate_by_partner.sort_values(ascending=False)

# Bar chart visualization
plot_bar_chart(
    data=interaction_rate_by_partner.head(10),
    title="Customer Service Interaction Rates by Partner",
    xlabel="Partner",
    ylabel="Interaction Rate (%)",
    color="blue",
    rotation=45
)


# Analyze errand categories by partner
errand_categories_by_partner = merged_df.groupby("Partner")["Errand_category"].value_counts(normalize=True).unstack()
#print("Errand Categories by Partner:")
#print(errand_categories_by_partner)

# Visualize errand categories for a specific partner
specific_partner = "Partner CO"  # An example
plot_bar_chart(
    data=errand_categories_by_partner.loc[specific_partner],
    title="Errand Categories for {specific_partner}",
    xlabel="Errand Category",
    ylabel="Percentage of Total Contacts",
    color="orange",
    rotation=45
)


# Deduplicate orders in merged_df
merged_df_origin = merged_df.drop_duplicates(subset=["Order_id", "Origin_country"])
merged_df_destination = merged_df.drop_duplicates(subset=["Order_id", "Destination_country"])

# Calculate total unique orders by origin and destination
total_orders_by_origin = orders_df.groupby("Origin_country")["Order_id"].nunique()
total_orders_by_destination = orders_df.groupby("Destination_country")["Order_id"].nunique()

# Calculate unique contacts by origin and destination
contacts_by_origin = merged_df_origin.groupby("Origin_country")["Order_id"].nunique()
contacts_by_destination = merged_df_destination.groupby("Destination_country")["Order_id"].nunique()

# Calculate interaction rates by origin
interaction_rate_by_origin = (contacts_by_origin / total_orders_by_origin).fillna(0) * 100


# Calculate interaction rates by destination
interaction_rate_by_destination = (contacts_by_destination / total_orders_by_destination).fillna(0) * 100

# Visualize interaction rates by origin country

plot_bar_chart(
    data=interaction_rate_by_origin.sort_values(ascending=False).head(10),
    title="Top 10 Customer Service Interaction Rates by Origin Country",
    xlabel="Origin Country",
    ylabel="Interaction Rate (%)",
    color="blue",
    rotation=45
)
# Visualize interaction rates by destination country
plot_bar_chart(
    data=interaction_rate_by_destination.sort_values(ascending=False).head(10),
    title="Top 10 Customer Service Interaction Rates by Destination Country",
    xlabel="Destination Country",
    ylabel="Interaction Rate (%)",
    color="orange",
    rotation=45
)


# Top countries with high interaction rates
top_countries = interaction_rate_by_origin.sort_values(ascending=False).head(10).index.tolist()

# Filter merged data for top countries
filtered_data = merged_df[
    (merged_df['Origin_country'].isin(top_countries)) |
    (merged_df['Destination_country'].isin(top_countries))
]

# Group by country and errand category
errand_category_by_country = filtered_data.groupby(
    ['Origin_country', 'Errand_category']
).size().unstack(fill_value=0)

# Get top 10 errand categories with the highest total contacts across all countries
top_10_errands = errand_category_by_country.sum(axis=0).nlargest(10).index

# Filter data for the top 10 errand categories
errand_category_top_10_errands = errand_category_by_country[top_10_errands]

# Normalize for percentage distribution
errand_category_percentages = errand_category_top_10_errands.div(
    errand_category_top_10_errands.sum(axis=1), axis=0
)

# Plot errand categories for each country
# Plot errand categories for each country
#plt.figure(figsize=(14, 8))
errand_category_top_10_errands.T.plot(kind='bar', stacked=True, figsize=(14, 8), colormap='tab20')

# Customize the chart
plt.title("Top 10 Errand Categories with Highest Contacts Across Countries", fontsize=16)
plt.xlabel("Errand Category", fontsize=14)
plt.ylabel("Number of Contacts", fontsize=14)
plt.legend(title="Country", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.xticks(rotation=45)
plt.tight_layout()
plt.grid(axis='y', linestyle='--', alpha=0.7)


#Save the plot as an image file
#plt.savefig('Top 10 Errand Categories with Highest Contacts Across Countries.png')

#show the plot
#plt.show()
plt.close()  # Close this figure if it's not needed




# Determine which specific routes generate the most customer service contacts
orders_df["route"] = orders_df["Origin_country"] + " -> " + orders_df["Destination_country"]
merged_df["route"] = merged_df["Origin_country"] + " -> " + merged_df["Destination_country"]

# Ensure unique orders and contacts (here are more contacts recorded than orders for certain routes)
# Ensure unique orders
unique_orders = orders_df.drop_duplicates(subset=['Order_id'])

# Count total orders by route
total_orders_by_route = unique_orders["route"].value_counts()

# Count contacts by route (using Order_ID and route to ensure uniqueness)
unique_contacts = merged_df.drop_duplicates(subset=['Order_id', 'route'])
contacts_by_route = unique_contacts["route"].value_counts()

# Calculate interaction rates by route
interaction_rate_by_route = (contacts_by_route / total_orders_by_route).fillna(0) * 100
interaction_rate_by_route = interaction_rate_by_route.sort_values(ascending=False)



# Visualize the top 10 high-contact routes
plot_bar_chart(
    data=interaction_rate_by_route.head(10),
    title="Top 10 Routes with Highest Customer Service Interaction Rates",
    xlabel="Route (Origin -> Destination)",
    ylabel="Interaction Rate (%)",
    color="purple",
    rotation=15
)


# Analyze cancellation rates by origin and destination
cancellation_by_origin = orders_df[orders_df["Is_canceled"] == 1]["Origin_country"].value_counts()
cancellation_by_destination = orders_df[orders_df["Is_canceled"] == 1]["Destination_country"].value_counts()

# Visualize cancellations by origin country
plot_bar_chart(
    data=cancellation_by_origin.sort_values(ascending=False).head(10),
    title="Top 10 Origin Countries with Highest Cancellation Rates",
    xlabel="Origin Country",
    ylabel="Number of Cancellations",
    color="red",
    rotation=15
)
# Identify the most frequent reasons for order cancellations
cancel_reasons = orders_df["Cancel_reason"].value_counts()

# Visualize cancellation reasons
plot_bar_chart(
    data=cancel_reasons.head(10),
    title="Top 10 Cancellation Reasons",
    xlabel="Cancellation Reason",
    ylabel="Frequency",
    color="red",
    rotation=45
)
# Identify the most common reasons for booking modifications
change_reasons = orders_df["Change_reason"].value_counts()
#print("Top Change Reasons:\n", change_reasons)

# Visualize change reasons
plot_bar_chart(
    data=change_reasons.head(5),
    title="Top 5 Change Reasons",
    xlabel="Change Reason",
    ylabel="Frequency",
    color="blue",
    rotation=10
)