# Google Cloud Data Engineering project: ELT pipeline with Cloud Run function, Cloud Composer, DBT, Cloud Run, Daraform, BigQuery, Cloud Build 

<img width="1008" height="644" alt="image" src="https://github.com/user-attachments/assets/3ec50eec-46e6-4663-95fc-de49715a3d2a" />

This ELT project was inspired by the desire to learn how to use dbt on Google Cloud. The simplest and most cost-effective approach is to run dbt as a Cloud Run job.

Additionally, I came up with the idea to switch to Dataform, a GCP-native tool that offers similar functionality to dbt, so I tested this option here as well.

# Cloud Run - Cloud Build - Artifact Registry

First, write your dbt files and the Dockerfile. Next, build your image in Cloud Build and push this image to Artifact Registry. After that, deploy the image to Cloud Run and create a job.

Here how my Docker images look like in Artifact Registry.

<img width="1915" height="912" alt="Screenshot (49)" src="https://github.com/user-attachments/assets/555fd1a6-c8f4-4b6e-866d-4e3bc9be7f51" />

Here is my Cloud Build history.

<img width="1916" height="908" alt="Screenshot (48)" src="https://github.com/user-attachments/assets/819743d3-7644-4ee9-ac9e-7949b2d460dc" />

Here are my 3 Cloud Run jobs:

    dbt-test-raw-job: Runs tests on raw data that has been loaded into BigQuery. Deploy this job: gcloud builds submit --config cloudbuild.test.raw.yml .
    dbt-transform-job: Executes transformations on raw data in BigQuery. Deploy this job: gcloud builds submit --config cloudbuild.transform.yml .
    dbt-test-transformed-job: Conducts tests on the transformed data in BigQuery. Deploy this job: gcloud builds submit --config cloudbuild.test.transformed.yml .


<img width="1916" height="908" alt="Screenshot (43)" src="https://github.com/user-attachments/assets/54a0b894-c1a8-4857-8c53-e3d2dd0bd880" />

Click on the job to view the details. Click on 'Execute' to start the job manually. Click on 'View' to open the logs.

<img width="1920" height="914" alt="Screenshot (46)" src="https://github.com/user-attachments/assets/28c2f525-e0f0-4300-875d-4900a499d12b" />

The logs will display all processes and results.

<img width="1911" height="918" alt="Screenshot (47)" src="https://github.com/user-attachments/assets/bd97c68f-be58-4fe5-a3a5-c8dc94e11d25" />

# BigQuery raw/transformed data

Here is what the schema of the loaded raw data looks like in BigQuery.

<img width="1917" height="911" alt="Screenshot (40)" src="https://github.com/user-attachments/assets/df5d5d37-cc7d-438f-bf3f-8471a66ce987" />

And this is what the schema of the transformed data looks like.

<img width="1915" height="904" alt="Screenshot (44)" src="https://github.com/user-attachments/assets/7afa2126-b2e5-4670-996a-38685d26bdab" />






