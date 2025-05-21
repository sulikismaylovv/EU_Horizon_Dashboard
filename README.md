# Horizon Europe Interactive Dashboard

This project develops an interactive Python-based dashboard for exploring, visualizing, and analyzing Horizon Europe research projects funded by the European Union (2021–2027). The dashboard facilitates exploration of funding distributions, collaboration networks, thematic and temporal trends, and project outcomes, providing valuable insights for analysts, policymakers, and stakeholders.

---

## Project Structure

```
EU_HORIZON_DASHBOARD/
│
├── backend/                         # All backend/data logic lives here
│   ├── etl/                         # Data ingestion, cleaning, transformation, load
│   │   ├── __init__.py
│   │   ├── ingestion.py
│   │   ├── cleaning.py
│   │   ├── transform.py
│   │   ├── load_to_db.py
│   │   └── ...
│   │
│   ├── db/                          # Database schema, migration, and connection logic
│   │   ├── __init__.py
│   │   ├── schema.sql               # SQL to create tables
│   │   ├── supabase_client.py       # Handles connections, inserts, etc.
│   │   └── migrate.py               # Optional: migration scripts
│   │
│   ├── models/                      # ML/NLP models, pipelines, forecasting, etc.
│   │   ├── __init__.py
│   │   ├── topic_modelling.py
│   │   ├── forecasting.py
│   │   └── ...
│   │
│   ├── api/                         # Custom API (FastAPI, Flask, etc.)
│   │   ├── __init__.py
│   │   ├── main.py                  # Entrypoint
│   │   └── endpoints/               # (Optional) subfolder per endpoint
│   │
│   ├── utils/                       # General utilities/helpers
│   │   ├── __init__.py
│   │   ├── save_load.py
│   │   └── viz_utils.py
│   │
│   ├── config.py
│   └── preprocess_data.py           # Main orchestration for ETL pipeline
│
├── frontend/                        # Modern dashboard (React, Next.js, Shiny, Streamlit, etc.)
│   ├── public/
│   ├── src/
│   ├── package.json
│   └── ... (JS/CSS/TSX files)
│
├── data/                            # Data, never version control large files
│   ├── raw/
│   ├── interim/
│   └── processed/
│
├── notebooks/                       # Prototyping, profiling, exploration (never for prod code)
│   ├── 01_data_preparation.ipynb
│   └── ...
│
├── tests/                           # Automated tests (pytest, unit tests)
│   ├── backend/
│   └── frontend/
│
├── .env                             # Secrets (never commit!)
├── .gitignore
├── README.md
├── requirements.txt                 # Python requirements for backend/ETL
├── environment.yml                  # (Optional) Conda env for reproducibility
├── LICENSE
└── docker-compose.yml               # (Optional) Orchestrate backend/frontend locally

```

---

## Installation and Setup

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/horizon-dashboard.git
cd horizon-dashboard
```

### 2. Set Up Python Environment

It's recommended to use a virtual environment.

```bash
python -m venv env
source env/bin/activate  # On Windows: env\Scripts\activate
pip install -r requirements.txt
```

### 3. Data Preparation

Place your downloaded dataset files into the `data/raw/` folder. Run preprocessing scripts:

```bash
python scripts/preprocess_data.py
```

### 4. Running the Dashboard

To run the Dash application locally:

```bash
python app/app.py
```

Navigate to `http://localhost:8050/` in your browser.

---

## Dashboard Features

- **Funding Allocation Dashboard:** Interactive maps and charts of EU research funding.
- **Collaboration Network Dashboard:** Visualize institutional and national collaboration patterns.
- **Project Outcomes Dashboard:** Analysis of publications and deliverables.
- **Thematic and Temporal Trends Dashboard:** Explore thematic evolutions and yearly trends.
- **Predictive Analytics:** Forecast funding and thematic trends.

---

## Deployment

The dashboard is deployable using Docker:

```bash
# Build Docker image
docker build -t horizon-dashboard -f deployment/Dockerfile .

# Run Docker container
docker run -p 8050:8050 horizon-dashboard
```

Access the dashboard via `http://localhost:8050/`.

---

## Testing

Execute tests to ensure functionality:

```bash
pytest tests/
```

---

## Documentation

Detailed project documentation and user guides can be found in the `docs/` directory.

---

## Resources and Example Projects

- [European Commission Horizon Dashboard](https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/horizon-dashboard)
- [CORDIS EU Research Results](https://cordis.europa.eu/)
- [CORDIS Datalab](https://cordis.europa.eu/datalab/datalab.php)
- [Horizon 2020 Dashboard](https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/horizon-dashboard)

---

## Contributing

Contributions to enhance this project are welcome. Please create pull requests and clearly document your changes.

---

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.
