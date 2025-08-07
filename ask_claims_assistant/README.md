# Ask claims assistant

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