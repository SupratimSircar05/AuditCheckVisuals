import json
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
import streamlit as st

# Step 1: Create a SQLAlchemy engine
engine = sqlalchemy.create_engine(
    "mysql+mysqlconnector://root:July123#@52.87.123.75:9782/health"
)

period = 30  # in days

# Step 2: Calculate the date 'period days' away from today
duration = datetime.now() - timedelta(days=period - 1)

# Step 3: Retrieve the data for 'Detect and Locate' based on period
query = f"""
SELECT * FROM health.dnasofferStatus_new
WHERE NAME = 'Detect and Locate'
AND DATE >= '{duration.strftime('%Y-%m-%d %H:%M:%S')}'
ORDER BY DATE DESC;
"""
df = pd.read_sql(query, engine)

# Step 4: Parse the 'reason' column
def parse_reason(reason):
    try:
        data = json.loads(reason)
        return {
            "Clients_Device": data.get("Clients_Device", {}).get("count", None),
            "Tag_Device": data.get("Tag_Device", {}).get("count", None),
            "BLE_Tags": data.get("BLE_Tags", {}).get("count", None)
        }
    except json.JSONDecodeError:
        return {
            "Clients_Device": None,
            "Tag_Device": None,
            "BLE_Tags": None
        }

df["parsed_reason"] = df["reason"].apply(parse_reason)
df = pd.concat(
    [df.drop(["reason"], axis=1), df["parsed_reason"].apply(pd.Series)], axis=1
)

# Ensure 'date' column is in datetime format
df['date'] = pd.to_datetime(df['date'])

# Step 5: Create a Streamlit dashboard
st.title(
    f"Firehose Data Pipeline ({period} days)\nPeriod: {duration.strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}")

# Calculate and display total runs
total_runs = len(df)
st.metric("Number of times script executed", value=total_runs)

# Calculate average availability
avg_clients_device = df["Clients_Device"].mean()
avg_tag_device = df["Tag_Device"].mean()
avg_ble_tags = df["BLE_Tags"].mean()

# Use columns to organize metrics
col1, col2, col3 = st.columns(3)
col1.metric("Avg Clients_Device received", value=f"{avg_clients_device:.2f}")
col2.metric("Avg Tag_Device received", value=f"{avg_tag_device:.2f}")
col3.metric("Avg BLE_Tags received", value=f"{avg_ble_tags:.2f}")

# Display the main data as a line chart
st.line_chart(
    df.set_index("date")[["Clients_Device", "Tag_Device", "BLE_Tags"]]
)

# Display subplots for each field
st.header("Individual Data Trends")

# Clients_Device Trend
st.subheader("Clients_Device")
st.line_chart(df.set_index("date")["Clients_Device"])

# Tag_Device Trend
st.subheader("Tag_Device")
st.line_chart(df.set_index("date")["Tag_Device"])

# BLE_Tags Trend
st.subheader("BLE_Tags")
st.line_chart(df.set_index("date")["BLE_Tags"])

# Step 6: Restructure data for grid display with dates as rows and services as columns
st.header("Expanded Daily Breakup")

# Ensure 'date' column is in datetime format and extract only the date part
df['date'] = pd.to_datetime(df['date']).dt.date

# Group by date and calculate daily averages for each field
daily_data = df.groupby('date')[["Clients_Device", "Tag_Device", "BLE_Tags"]].mean()

# Get the list of unique dates to use as rows
dates = daily_data.index.tolist()

# Function to apply color based on value
def color_cell(value):
    if value is None:
        return "background-color: grey"
    elif value < 100:
        return "background-color: red"
    else:
        return "background-color: green"

# Format the data as a string for Streamlit
grid_output = ""

# Header row with Services
header_row = "| Date | Clients_Device | Tag_Device | BLE_Tags |\n"
grid_output += header_row
grid_output += "| --- | --- | --- | --- |\n"

# Data rows with dates
for date in dates:
    clients_device_value = daily_data.loc[date, 'Clients_Device']
    tag_device_value = daily_data.loc[date, 'Tag_Device']
    ble_tags_value = daily_data.loc[date, 'BLE_Tags']

    date_row = f"| {date} |"
    date_row += f' <div style="text-align: center; {color_cell(clients_device_value)}">{clients_device_value:.2f}</div> |' if clients_device_value is not None else ' <div style="text-align: center; background-color: grey">N/A</div> |'
    date_row += f' <div style="text-align: center; {color_cell(tag_device_value)}">{tag_device_value:.2f}</div> |' if tag_device_value is not None else ' <div style="text-align: center; background-color: grey">N/A</div> |'
    date_row += f' <div style="text-align: center; {color_cell(ble_tags_value)}">{ble_tags_value:.2f}</div> |' if ble_tags_value is not None else ' <div style="text-align: center; background-color: grey">N/A</div> |'
    grid_output += date_row + "\n"

# Display the formatted grid in Streamlit
st.markdown(grid_output, unsafe_allow_html=True)

# Add a legend below the table for clarity
st.markdown("""
### Legend:
- **Green**: Value >= 100 (Good)
- **Red**: Value < 100 (Needs Attention)
- **Grey**: No Data
""", unsafe_allow_html=True)
