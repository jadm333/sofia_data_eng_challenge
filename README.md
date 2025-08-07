# Sofia Data Eng Challenge
José Antonio Díaz Martínez (jadm333@gmail.com)

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

3. (Optional) Create and fill the `.env` file with the folder id

4. (Optional) Download Data:
```bash
python download_data.py
```

5. (Optional) The abvove method to get the data is optional, you can just download the data manually and put the csv files inside of `data_modelling/raw_data`


## Project Overview
The project is divided in two parts the `data_modelling`, which creates the DB and other files that will use the second part, which is the `ask_claims_assistant`. In each folder are `README.md` files with instructions on how to run it. Consider that the ask claims assistant will only work after the data modelling part is run.