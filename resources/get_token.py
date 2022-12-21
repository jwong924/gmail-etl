from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
import os

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
SERVICE_ACCOUNT_FILE = 'jonpi_service_account.json'

def get_token():
    creds = None
    # Use token if exists
    if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, create token with service account
    if not creds or not creds.valid:
        # If credentials exist, refresh token
        if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
        # Create new token.json file with service account credentials
        else:
            creds = service_account.Credentials.from_service_account_file(
                filename=SERVICE_ACCOUNT_FILE,
                scopes=SCOPES
            )
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
    try:
        # Call the Gmail API
        service = build('gmail', 'v1', credentials=creds)
        response = service.users().getProfile(userId='me').execute()
        print(response)


    except HttpError as error:
        # TODO(developer) - Handle errors from gmail API.
        print(f'An error occurred: {error}')
        
    return creds.to_json()

