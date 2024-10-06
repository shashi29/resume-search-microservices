import streamlit as st
import sqlite3
import pandas as pd
import json
from datetime import datetime

# Function to connect to the database
def get_db_connection():
    conn = sqlite3.connect('/root/code/resume-search-microservices/operations.db')
    conn.row_factory = sqlite3.Row
    return conn

# Function to get all operations
def get_all_operations():
    conn = get_db_connection()
    operations = conn.execute('SELECT * FROM operations ORDER BY timestamp DESC').fetchall()
    conn.close()
    return operations

# Function to search operations
def search_operations(search_term):
    conn = get_db_connection()
    operations = conn.execute('SELECT * FROM operations WHERE id LIKE ? OR operation LIKE ? OR status LIKE ? ORDER BY timestamp DESC', 
                              ('%' + search_term + '%', '%' + search_term + '%', '%' + search_term + '%')).fetchall()
    conn.close()
    return operations

# Set page title
st.set_page_config(page_title="Operation Status Tracker", page_icon="📊", layout="wide")

# Main title
st.title("📊 Operation Status Tracker")

# Search box
search_term = st.text_input("Search operations (by ID, operation type, or status):", "")

if search_term:
    operations = search_operations(search_term)
else:
    operations = get_all_operations()

# Convert operations to a DataFrame
df = pd.DataFrame(operations, columns=['id', 'operation', 'status', 'details', 'timestamp'])
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Display summary statistics
st.subheader("Summary Statistics")
col1, col2, col3 = st.columns(3)
col1.metric("Total Operations", len(df))
col2.metric("Completed Operations", len(df[df['status'] == 'COMPLETED']))
col3.metric("Error Operations", len(df[df['status'] == 'ERROR']))

# Display operations table
st.subheader("Operations Table")
st.dataframe(df[['id', 'operation', 'status', 'timestamp']], use_container_width=True)

# Display detailed information for selected operation
st.subheader("Operation Details")
selected_operation = st.selectbox("Select an operation to view details:", df['id'])
if selected_operation:
    operation_details = df[df['id'] == selected_operation].iloc[0]
    st.write(f"**Operation ID:** {operation_details['id']}")
    st.write(f"**Operation Type:** {operation_details['operation']}")
    st.write(f"**Status:** {operation_details['status']}")
    st.write(f"**Timestamp:** {operation_details['timestamp']}")
    
    details = json.loads(operation_details['details'])
    st.json(details)

# Display status distribution chart
st.subheader("Status Distribution")
status_counts = df['status'].value_counts()
st.bar_chart(status_counts)

# Display operations over time
st.subheader("Operations Over Time")
df_grouped = df.groupby(df['timestamp'].dt.date).size().reset_index(name='count')
st.line_chart(df_grouped.set_index('timestamp')['count'])

# Footer
st.markdown("---")
st.markdown("📊 Operation Status Tracker - Powered by Streamlit")