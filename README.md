# Sofia Data Eng Challenge



1. **Create Conda Environment:**
```bash
conda env create -f conda_env.yml
conda activate sofia_data_eng_challenge
```

2. (Optional) Configure Google Drive API Credentials:
    - Go to the [Google Cloud Console](https://console.cloud.google.com/).
    - Create a new project.
    - Enable the Google Drive API.
    - Create credentials for a service account.
    - Download the JSON key file and save it as `gdrive_credentials.json` in the root of this project.
    - Share the Google Drive folder containing the CSV files with the service account's email address.

3. (Optional) Download Data:
```bash
python download_data.py
```

4. (Optional) The abvove method to get the data is optional, you can just download the data manually and put the csv files inside of `data_modelling/raw_data`