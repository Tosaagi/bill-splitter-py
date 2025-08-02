import azure.functions as func
import os
import uuid
import logging
import json
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from azure.data.tables import TableClient
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import ResourceNotFoundError
from datetime import datetime, timedelta

# ========================================================================
#  1. DEFINE THE APP & GET CREDENTIALS
#
#  All functions will be attached to this single 'app' object.
# ========================================================================
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Get credentials from environment variables
STORAGE_CONNECT_STR = os.getenv("billsplitterstorage0725_STORAGE")
DOC_INTELLIGENCE_ENDPOINT = os.getenv("DOC_INTELLIGENCE_ENDPOINT")
DOC_INTELLIGENCE_KEY = os.getenv("DOC_INTELLIGENCE_KEY")

# Constants
RECEIPT_CONTAINER_NAME = "receipts"
RESULTS_TABLE_NAME = "receiptresults"

# ========================================================================
#  2. GET UPLOAD URL FUNCTION
# ========================================================================
@app.route(route="get_upload_url", methods=["GET"])
def get_upload_url(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Request received for a SAS upload URL.')
    try:
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECT_STR)
        blob_name = f"{uuid.uuid4()}.jpg"
        
        sas_token = generate_blob_sas(
            account_name=blob_service_client.account_name,
            container_name=RECEIPT_CONTAINER_NAME,
            blob_name=blob_name,
            account_key=blob_service_client.credential.account_key,
            permission=BlobSasPermissions(write=True),
            expiry=datetime.utcnow() + timedelta(minutes=5)
        )
        sas_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{RECEIPT_CONTAINER_NAME}/{blob_name}?{sas_token}"
        
        return func.HttpResponse(
            body=json.dumps({"sasUrl": sas_url, "blobName": blob_name}),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error generating SAS URL: {e}")
        return func.HttpResponse("Failed to generate upload URL.", status_code=500)

# ========================================================================
#  3. GET RECEIPT RESULTS FUNCTION
# ========================================================================
@app.route(route="get_receipt_results/{blobName}")
def get_receipt_results(req: func.HttpRequest) -> func.HttpResponse:
    blob_name = req.route_params.get('blobName')
    if not blob_name:
        return func.HttpResponse("Please provide a blobName.", status_code=400)
    
    receipt_id = os.path.splitext(blob_name)[0]
    logging.info(f"Request received for receipt results for ID: {receipt_id}")

    try:
        table_client = TableClient.from_connection_string(STORAGE_CONNECT_STR, RESULTS_TABLE_NAME)
        entity = table_client.get_entity(partition_key="receipt", row_key=receipt_id)

        if 'items' in entity and isinstance(entity['items'], str):
            entity['items'] = json.loads(entity['items'])

        return func.HttpResponse(body=json.dumps(entity, default=str), mimetype="application/json", status_code=200)
        
    except ResourceNotFoundError:
        return func.HttpResponse("Not found.", status_code=404)
    except Exception as e:
        logging.error(f"Error fetching from table: {e}")
        return func.HttpResponse("Error fetching results.", status_code=500)

# ========================================================================
#  4. ANALYZE RECEIPT FUNCTION (BLOB TRIGGER)
# ========================================================================
@app.blob_trigger(arg_name="blob", path="receipts/{name}", connection="billsplitterstorage0725_STORAGE")
@app.table_output(arg_name="tableOutput", tableName=RESULTS_TABLE_NAME, connection="billsplitterstorage0725_STORAGE")
def AnalyzeReceipt(blob: func.InputStream, tableOutput: func.Out[str]):
    receipt_id = os.path.splitext(blob.name.split('/')[-1])[0]
    logging.info(f"AnalyzeReceipt triggered for blob: {blob.name}")

    try:
        doc_analysis_client = DocumentAnalysisClient(DOC_INTELLIGENCE_ENDPOINT, AzureKeyCredential(DOC_INTELLIGENCE_KEY))
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECT_STR)
        
        sas_token = generate_blob_sas(
            account_name=blob_service_client.account_name,
            container_name=RECEIPT_CONTAINER_NAME,
            blob_name=blob.name.split('/')[-1],
            account_key=blob_service_client.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(minutes=5)
        )
        sas_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{blob.name}?{sas_token}"
        
        poller = doc_analysis_client.begin_analyze_document_from_url("prebuilt-receipt", sas_url)
        result = poller.result()

        if result.documents:
            receipt = result.documents[0].fields
            items_list = []
            if receipt.get("Items") and receipt.get("Items").value:
                for item in receipt.get("Items").value:
                    item_props = item.value
                    items_list.append({
                        "description": item_props.get("Description", {}).value,
                        "totalPrice": item_props.get("TotalPrice", {}).value or 0
                    })
            
            calculated_subtotal = round(sum(item['totalPrice'] for item in items_list), 2)
            
            output_data = {
                "PartitionKey": "receipt",
                "RowKey": receipt_id,
                "items": json.dumps(items_list),
                "subtotal": calculated_subtotal,
                "tax": receipt.get("TotalTax", {}).value or 0,
                "total": receipt.get("Total", {}).value or 0
            }
            
            tableOutput.set(json.dumps(output_data))
            logging.info(f"Successfully processed receipt data for: {receipt_id}")
    except Exception as e:
        logging.error(f"Error in AnalyzeReceipt: {e}")