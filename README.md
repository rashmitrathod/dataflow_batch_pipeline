1. Enable API:
   - Dataflow API
   - Data pipelines API

2. Create Artifact repository as mentioned below
   - europe-west2-docker.pkg.dev/rk-poc-project/rktest-artifactory

3. Create two GCS Buckets
   - gs://rk-poc-project_cloudbuild (to store dataflow template file i.e. gcstobigquery.json)
   - gs://rk-poc-project-logs-bucket (to store dataflow job logs)

4. Create BigQuery dataset and two table inside dataset
   - dataflow_pipeline (dataset)
   - gcs_bq_test - use resources/gcs_bq_test.json table schema to to create this table
   - gcs_bq_error_table - use resources/gcs_bq_error_table.json table schema to to create this table

5. Ensure your VPC's subnet has PGA (Private Google Access) is ON

6. Build Dataflow Flex template using below command

gcloud dataflow flex-template build gs://rk-poc-project_cloudbuild/gcstobigquery.json  --image-gcr-path "europe-west2-docker.pkg.dev/rk-poc-project/rktest-artifactory/gcstobigquery:latest" --sdk-language "JAVA" --flex-template-base-image "JAVA11" --metadata-file "metadata.json" --jar "target/pipeline-0.0.1.jar" --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.example.dataflow.pipeline.GcsToBigQuery" --gcs-log-dir="gs://rk-poc-project-logs-bucket" --project rk-poc-project

7. Deploy and run Dataflow job using below command
gcloud dataflow flex-template run "gcs-to-bq" --template-file-gcs-location "gs://rk-poc-project_cloudbuild/gcstobigquery.json" --parameters projectID=rk-poc-project --parameters outputTable=rk-poc-project:dataflow_pipeline.gcs_bq_test --parameters errorSpec=rk-poc-project:dataflow_pipeline.gcs_bq_error_table --parameters sourceFilePath=gs://rk-poc-project-config/gcs_bq_csv.csv --parameters csvHeaders="ID~Name~LastName~Marks~Percentage" --parameters sourceName="studentData" --region "europe-west2" --network "default" --service-account-email "rk-owner-sa@rk-poc-project.iam.gserviceaccount.com" --subnetwork "https://www.googleapis.com/compute/v1/projects/rk-poc-project/regions/europe-west2/subnetworks/default" --num-workers=1 --disable-public-ips --project rk-poc-project


