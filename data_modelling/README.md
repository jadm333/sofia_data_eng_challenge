# Data Modelling

This project uses dbt and DuckDB to model data.

## Setup

1. Activate conda environment
```bash
conda activate sofia_data_eng_challenge
```

2. Ingest seed data, this means load the raw_data csv's files into duckdb database
```bash
dbt seed
```

3. Run dbt:
```bash
dbt run
```

4. Test dbt:
```bash
dbt test
```

5. Generate the dbt docs
```bash
dbt docs generate
```
