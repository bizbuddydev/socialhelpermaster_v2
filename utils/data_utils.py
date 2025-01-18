import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

# Initialize BigQuery client with cached credentials
@st.cache_data
def initialize_bigquery_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials, project=st.secrets["gcp_service_account"]["project_id"])

# Generic function to execute a query and return results as a DataFrame
@st.cache_data
def fetch_data_from_bigquery(query, params=None):
    client = initialize_bigquery_client()
    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(key, "STRING", value)
                for key, value in (params or {}).items()
            ]
        )
        query_job = client.query(query, job_config=job_config)
        result = query_job.result()
        return result.to_dataframe()
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

# Function to fetch business description
@st.cache_data
def pull_business_description(project_id, dataset_id, table_id):
    query = f"""
        SELECT `Description of Business and Instagram Goals`
        FROM `{project_id}.{dataset_id}.{table_id}`
        LIMIT 1
    """
    return fetch_data_from_bigquery(query)

# Function to fetch post ideas
@st.cache_data
def pull_post_ideas(project_id, dataset_id, table_id, limit=3):
    query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
        LIMIT {limit}
    """
    return fetch_data_from_bigquery(query)

# Function to fetch account summary
@st.cache_data
def pull_account_summary(project_id, dataset_id, table_id, page_id):
    query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE page_id = @page_id
        ORDER BY date DESC
        LIMIT 1
    """
    params = {"page_id": page_id}
    return fetch_data_from_bigquery(query, params)

# Function to fetch generic table data
@st.cache_data
def pull_table_data(project_id, dataset_id, table_id):
    query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
    """
    return fetch_data_from_bigquery(query)
