import streamlit as st
import pandas as pd
import tempfile
from gretel_client import configure_session
from gretel_client.projects import create_or_get_unique_project
from gretel_client.helpers import poll
import os
from faker import Faker
import random
import ssl  # Added for SSL workaround

page_title = "Data_Sys"
page_icon = ":note:"
layout = "centered"

st.set_page_config(page_title=page_title, page_icon=page_icon, layout=layout)
col1, col2, col3 = st.columns(3)

with col1:
    st.write(' ')

with col2:
    st.image("images.jpeg")

with col3:
    st.write(' ')

st.markdown("<h1 style='text-align: center; color: white;'>Better data makes better models.</h1>", unsafe_allow_html=True)

hide_st_style = """
        <style>
        #MainMenu{visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        </style>"""
st.markdown(hide_st_style, unsafe_allow_html=True)

# Initialize Faker instance
faker = Faker()

# Function to synthesize data based on column name context
# Updated function to synthesize data generically based on data type
# Function to synthesize data based on column name context
# Updated function to synthesize column data based on column names and types
def synthesize_column_data(df):
    for col in df.columns:
        col_lower = col.lower()
        
        # Specific checks based on column names
        if 'name' in col_lower:  # For names (person or organization)
            df[col] = df[col].apply(lambda x: faker.name() if 'person' in col_lower else faker.company())
        elif 'address' in col_lower:  # Addresses
            df[col] = df[col].apply(lambda x: faker.address())
        elif 'email' in col_lower:  # Emails
            df[col] = df[col].apply(lambda x: faker.email())
        elif 'phone' in col_lower or 'contact' in col_lower:  # Phone numbers
            df[col] = df[col].apply(lambda x: faker.phone_number())
        elif 'product' in col_lower:  # Product-related columns
            df[col] = df[col].apply(lambda x: faker.catch_phrase())
        elif 'company' in col_lower:  # Company-related columns
            df[col] = df[col].apply(lambda x: faker.company())
        elif 'state' in col_lower:  # Company-related columns
            df[col] = df[col].apply(lambda x: faker.state())
        elif 'Country Name' in col_lower:  # Company-related columns
            df[col] = df[col].apply(lambda x: faker.country())
        elif 'city' in col_lower:
            df[col] = df[col].apply(lambda x: faker.city())
        elif pd.api.types.is_string_dtype(df[col]):  # Generic string columns
            df[col] = df[col].apply(lambda x: faker.word())
        elif pd.api.types.is_numeric_dtype(df[col]):  # Numeric columns
            df[col] = df[col].apply(lambda x: synthesize_numeric_column(df[col]))
        elif pd.api.types.is_datetime64_any_dtype(df[col]):  # Date/Time columns
            df[col] = df[col].apply(lambda x: faker.date_between(start_date="-10y", end_date="today"))
        else:  # Fallback for unknown types
            df[col] = df[col].apply(lambda x: faker.word())
    
    return df


# Function to generate synthetic data for string columns
def synthesize_string_column():
    string_types = [
        faker.name,           # Names
        faker.email,          # Emails
        faker.phone_number,   # Phone numbers
        faker.address,        # Addresses
        faker.company,        # Company names
        faker.catch_phrase,   # Catch phrases
        faker.word            # Generic word
    ]
    return random.choice(string_types)()

# Function to generate synthetic data for numeric columns
def synthesize_numeric_column(column):
    if pd.api.types.is_integer_dtype(column):  # Integer column
        return faker.random_int(min=1, max=10000)
    elif pd.api.types.is_float_dtype(column):  # Float column
        return round(faker.pyfloat(left_digits=5, right_digits=2, positive=True), 2)


# Function to process the dataset and save the synthesized output
def synthesize_dataset_all(input_file, output_file):
    # Try reading the dataset with different encodings
    try:
        df = pd.read_csv(input_file, encoding='utf-8')
        st.success("File successfully read with UTF-8 encoding.")
    except UnicodeDecodeError:
        print("Failed to read the file with UTF-8 encoding. Trying 'ISO-8859-1'...")
        try:
            df = pd.read_csv(input_file, encoding='ISO-8859-1')
            st.success("File successfully read with ISO-8859-1 encoding.")
        except UnicodeDecodeError:
            st.error("Failed to read the file with both UTF-8 and ISO-8859-1 encodings.")
            return  # Exit the function if reading the file fails

    st.write("Original dataset:")
    st.write(df.head())

    # Synthesize data for all columns
    synthesized_df = synthesize_column_data(df)

    # Save the synthesized dataset to a new file
    synthesized_df.to_csv(output_file, index=False)
    st.write("\nSynthesized dataset saved to:", output_file)
    st.write("Synthesized dataset head:")
    st.write(synthesized_df.head())

# Gretel credentials should be configured here
configure_session(api_key='grtuc8d12f0eebee3b191c31d6f01f6157741404d93eccb4573a83dea110bc5d409f')

# Get or create a project
project = create_or_get_unique_project(name="Data-Synthesis")

# Load the model from the project
model = project.get_model(model_id="66ea631ce7598f36a9612bd1")

# Upload file for processing
data_source = st.file_uploader("Upload file", type=["csv"])

if data_source is not None:
    # Create a temporary file to store the uploaded file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
        temp_file.write(data_source.read())
        temp_file_path = temp_file.name

    # Apply data synthesis to the uploaded file
    output_file = temp_file_path.replace(".csv", "_synthesized.csv")
    synthesize_dataset_all(temp_file_path, output_file)

    # Workaround for SSL verification failure
    ssl._create_default_https_context = ssl._create_unverified_context

    # Create a record handler for the synthesized dataset
    record_handler = model.create_record_handler_obj(data_source=output_file)

    # Submit the record handler to the cloud
    record_handler.submit_cloud()

    # Poll the job until it's done
    poll(record_handler)

    # After transformation
    artifact_link = record_handler.get_artifact_link("data")

    # Load the transformed data
    transformed = pd.read_csv(artifact_link, compression="gzip")
    st.write("File head, after synthesis")
    st.write(transformed.head())

    # Provide download link for the transformed data
    transformed_csv = transformed.to_csv(index=False)
    st.download_button(
        label="Download Synthesized Data",
        data=transformed_csv,
        file_name="synthesized_data.csv",
        mime="text/csv"
    )

    # Cleanup the temporary file
    os.remove(temp_file_path)
