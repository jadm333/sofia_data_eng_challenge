# Ask claims assistant

**Important**: The assistant use some files generated bny dbt and also the duckdb file, be sure to run dbt before using the agent

Requirements:

1. Activate conda environment
```bash
conda activate sofia_data_eng_challenge
```

2. Create and fill the `.env` file with the google api key

Some example using the assistant:

```python
from assistant import ask_claims_mini_assistant
question = "Show me the trend of emergency room visits by month"
response = ask_claims_mini_assistant(question)
```

Response:

```json
{
    "natural_language_response": "Here is the trend of emergency room visits by month.",
    "sql_query": "SELECT STRFTIME(service_date, '%Y-%m') AS month,  COUNT(claim_id) AS number_of_visitsFROM stg_claimsWHERE  place_of_service = 'Emergency Room'GROUP BY  monthORDER BY  month;",
    "visualization_path": "tmp_plots/plot_32fniuwhef.png",
    "error_info": ""
}
```


See **more examples** in the `examples.ipynb`