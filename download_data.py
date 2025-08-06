import os
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Load environment variables from .env file
load_dotenv()

# Config
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_ACCOUNT_FILE = 'gdrive_credentials.json'
FOLDER_ID = os.getenv('GDRIVE_FOLDER_ID')
DOWNLOAD_PATH = 'data_modelling/raw_data'

def download_files_from_drive():
    """Downloads files from a Google Drive folder."""
    # Check if required environment variable is set
    if not FOLDER_ID:
        raise ValueError("GDRIVE_FOLDER_ID environment variable is not set. Please check your .env file.")
    
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)

    if not os.path.exists(DOWNLOAD_PATH):
        os.makedirs(DOWNLOAD_PATH)

    page_token = None
    while True:
        response = service.files().list(q=f"'{FOLDER_ID}' in parents",
                                          spaces='drive',
                                          fields='nextPageToken, files(id, name)',
                                          pageToken=page_token).execute()
        for file in response.get('files', []):
            print(f'Downloading {file.get("name")}')
            request = service.files().get_media(fileId=file.get('id'))
            fh = open(os.path.join(DOWNLOAD_PATH, file.get('name')), 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(f'Download {int(status.progress() * 100)}%.')

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

if __name__ == '__main__':
    download_files_from_drive()
