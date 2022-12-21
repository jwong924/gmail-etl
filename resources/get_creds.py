from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
import os

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
SERVICE_ACCOUNT_FILE = 'jonpi_service_account.json'

def get_creds():
    creds = service_account.Credentials.from_service_account_file(
        filename=SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )
    try:
        # Call the Gmail API
        service = build('gmail', 'v1', credentials=creds)
        response = service.users().getProfile(userId='me').execute()
        print(response)
        
    except HttpError as error:
        # TODO(developer) - Handle errors from gmail API.
        print(f'An error occurred: {error}')
        
    return creds