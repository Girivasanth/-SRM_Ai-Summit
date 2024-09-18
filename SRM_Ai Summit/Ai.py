import streamlit as st
import pandas as pd
import tempfile
from gretel_client import configure_session
from gretel_client.projects import create_or_get_unique_project
from gretel_client.helpers import poll
import os
from faker import Faker
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

faker = Faker()
# Gretel credentials should be configured here
configure_session(api_key="grtua6c72720ad7624cf15222329d025cae96e16ab66cebf9d24de13f692a12e58f1")

# Get or create a project
project = create_or_get_unique_project(name="Data-Synthesis")

# Load the model from the project
model = project.get_model(model_id="66e8823690bc85415f37a7e3")

def fake_pii_csv(filename, lines=100):
    fake = Faker()
    with open(filename, "w") as f:
        f.write("id,name,email,phone,visa,ssn,user_id\n")
        for i in range(lines):
            _name = fake.name()
            _email = fake.email()
            _phone = fake.phone_number()
            _cc = fake.credit_card_number()
            _ssn = fake.ssn()
            _id = f'user{fake.numerify(text="#####")}'
            f.write(f"{i},{_name},{_email},{_phone},{_cc},{_ssn},{_id}\n")

# Upload file for processing
data_source = st.file_uploader("Upload file", type=["csv"])

if data_source is not None:
    # Create a temporary file to store the uploaded file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
        temp_file.write(data_source.read())
        temp_file_path = temp_file.name

    # Call the fake_pii_csv function with the temporary file path
    fake_pii_csv(temp_file_path)

    # Create record handler using the temporary file path
    record_handler = model.create_record_handler_obj(data_source=temp_file_path)

    # Submit the record handler to the cloud
    record_handler.submit_cloud()

    # Poll the job until it's done
    poll(record_handler)

    # Compare results: Before transformation
    train_df = pd.read_csv(temp_file_path)
    st.write("Uploaded file head, before redaction")
    st.write(train_df.head())

    # Workaround for SSL verification failure
    ssl._create_default_https_context = ssl._create_unverified_context

    # After transformation
    artifact_link = record_handler.get_artifact_link("data")
    #st.write("Artifact link:", artifact_link)
    
    # Load the transformed data
    transformed = pd.read_csv(artifact_link, compression="gzip")
    st.write("File head, after redaction")
    st.write(transformed.head())

    # Provide download link for the transformed data
    transformed_csv = transformed.to_csv(index=False)
    st.download_button(
        label="Download Transformed Data",
        data=transformed_csv,
        file_name="transformed_data.csv",
        mime="text/csv"
    )

    # Cleanup the temporary file
    os.remove(temp_file_path)
