# EU Horizon Dashboard

An interactive full-stack web application for exploring, visualizing, and analyzing Horizon Europe research projects funded by the European Union (2021–2027). The dashboard provides comprehensive analytics on funding distributions, collaboration networks, thematic trends, and project outcomes through modern web technologies.

---

## 🏗️ Architecture

This project uses a modern **React + FastAPI** architecture:

- **Frontend**: React 19 with TypeScript, Plotly.js for visualizations, and Tailwind CSS
- **Backend**: FastAPI with Python 3.x, providing RESTful API endpoints
- **Database**: Supabase (PostgreSQL) for data storage and real-time capabilities
- **Data Processing**: ETL pipelines using pandas and custom Python scripts

## 🌐 Live Demo

- **Backend API**: [http://54.93.51.85:8000/](http://54.93.51.85:8000/)
- **API Documentation**: [http://54.93.51.85:8000/docs](http://54.93.51.85:8000/docs) (OpenAPI/Swagger UI)

---

## 📁 Project Structure

```
EU_Horizon_Dashboard/
│
├── backend/                         # Python backend with FastAPI
│   ├── fastAPI/                     # FastAPI application
│   │   ├── main.py                  # Main FastAPI app with all endpoints
│   │   └── routes/                  # Additional route modules
│   │
│   ├── etl/                         # Data processing pipeline
│   │   ├── ingestion.py            # Data ingestion from CORDIS
│   │   ├── cleaning.py             # Data cleaning and validation
│   │   ├── transform.py            # Data transformation and enrichment
│   │   └── load_to_db.py           # Database loading utilities
│   │
│   ├── db/                          # Database connection and management
│   │   ├── supabase_client.py      # Supabase connection setup
│   │   └── validate_schema.py      # Database schema validation
│   │
│   ├── models/                      # ML/AI models and predictions
│   │   ├── forecasting.py          # Funding forecasting models
│   │   └── train_predictive_models.py
│   │
│   ├── classes/                     # Data classes and models
│   │   ├── cordis_data.py          # CORDIS data handling
│   │   └── project_data.py         # Individual project analysis
│   │
│   ├── utils/                       # Utility functions
│   │   ├── plots.py                # Plotting utilities
│   │   ├── save_load.py            # File I/O operations
│   │   └── topic_modelling.py      # NLP and topic modeling
│   │
│   ├── config.py                    # Configuration management
│   ├── init_env.py                  # Environment initialization
│   └── preprocess_data.py           # Main ETL orchestrator
│
├── frontend/                        # React TypeScript frontend
│   ├── public/                      # Static assets
│   ├── src/                         # React components and logic
│   │   ├── components/              # Reusable React components
│   │   ├── App.tsx                  # Main application component
│   │   └── index.tsx                # Application entry point
│   ├── package.json                 # Node.js dependencies
│   ├── tsconfig.json                # TypeScript configuration
│   └── vercel.json                  # Vercel deployment config
│
├── data/                            # Data storage (not version controlled)
│   ├── raw/                         # Original CORDIS CSV files
│   ├── interim/                     # Intermediate processing results
│   └── processed/                   # Final processed data files
│
├── notebooks/                       # Jupyter notebooks for analysis
│   ├── 01_data_preparation.ipynb
│   ├── 02_funding_analysis.ipynb
│   ├── 03_collaboration_network.ipynb
│   ├── 04_thematic_trends.ipynb
│   └── 05_predictive_analysis.ipynb
│
├── supabase/                        # Supabase configuration
│   ├── config.toml
│   └── migrations/                  # Database migration files
│
├── main.py                          # CLI tool for data processing
├── requirements.txt                 # Python dependencies
└── README.md                        # This file
```

---

## 🚀 Quick Start

### Prerequisites

- **Python 3.8+** (for backend)
- **Node.js 16+** (for frontend)
- **Git** for version control

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/EU_Horizon_Dashboard.git
cd EU_Horizon_Dashboard
```

### 2. Backend Setup

```bash
# Create and activate Python virtual environment
python -m venv env
source env/bin/activate  # On Windows: env\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt
```

### 3. Frontend Setup

```bash
cd frontend
npm install
```

### 4. Environment Configuration

Create a `.env` file in the project root with your credentials:

```bash
SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_KEY=your_service_key
SUPABASE_ANON_KEY=your_anon_key
```

### 5. Data Processing (Optional)

If you have raw CORDIS data, process it using:

```bash
# Run the full ETL pipeline
python main.py preprocess

# Load processed data to database
python main.py load
```

### 6. Running the Application

**Backend (FastAPI):**
```bash
cd backend/fastAPI
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

**Frontend (React):**
```bash
cd frontend
npm start
```

The application will be available at:
- Frontend: [http://localhost:3000](http://localhost:3000)
- Backend API: [http://localhost:8000](http://localhost:8000)
- API Documentation: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## 🎯 Dashboard Features

### Analytics Endpoints

The backend provides comprehensive analytics through RESTful API endpoints:

- **🗺️ Interactive Maps**: Geographic visualization of funding and collaborations
- **📊 Funding Analysis**: Distribution analysis by country, institution, and program
- **🔗 Collaboration Networks**: Institutional and cross-border collaboration patterns
- **📈 Time Series Analysis**: Funding trends and project timelines
- **🎯 Thematic Analysis**: Research field distribution and evolution
- **🔍 Project Search**: Advanced filtering and project details
- **📚 Publication Analytics**: Research output and impact analysis
- **🤖 Predictive Models**: Funding forecasts and trend predictions

### Frontend Components

- **Interactive Map Component**: Real-time filtering and geographic visualization
- **Chart Gallery**: Various chart types using Plotly.js and Recharts
- **Data Tables**: Sortable and filterable project listings
- **Responsive Design**: Mobile-friendly interface with Tailwind CSS

---

## 🛠️ Technology Stack

### Backend
- **FastAPI**: Modern, fast web framework for building APIs
- **Python 3.x**: Core programming language
- **Pandas**: Data manipulation and analysis
- **Supabase**: PostgreSQL database with real-time features
- **Pydantic**: Data validation and serialization
- **Uvicorn**: ASGI server for FastAPI

### Frontend
- **React 19**: Modern JavaScript library for user interfaces
- **TypeScript**: Type-safe JavaScript development
- **Plotly.js**: Interactive scientific charting library
- **Recharts**: Composable charting library for React
- **Tailwind CSS**: Utility-first CSS framework
- **React Plotly.js**: React wrapper for Plotly.js

### Data & ML
- **Pandas**: Data processing and analysis
- **NumPy**: Numerical computing
- **Scikit-learn**: Machine learning algorithms
- **BERTopic**: Advanced topic modeling
- **NetworkX**: Network analysis and graph algorithms

---

## 📡 API Reference

### Base URLs
- **Production**: `http://54.93.51.85:8000`
- **Local Development**: `http://localhost:8000`

### Key Endpoints

```bash
# Interactive Map Data
GET /analytics/interactive-map

# Funding Analysis
GET /analytics/funding-by-country
GET /analytics/ec-by-country

# Project Information
GET /projects
GET /projects/{project_id}

# Collaboration Networks
GET /analytics/collaboration-network

# Research Analytics
GET /analytics/research-field-network
GET /analytics/topic-evolution

# Time Series Data
GET /analytics/project-timeline
GET /analytics/participation-trends
```

For complete API documentation, visit the interactive docs at `/docs` endpoint.

---

## 🔧 Development

### Running Tests

```bash
# Backend tests
cd backend
python -m pytest tests/

# Frontend tests
cd frontend
npm test
```

### Code Quality

```bash
# Python code formatting
black backend/
flake8 backend/

# TypeScript/React linting
cd frontend
npm run lint
```

### Database Management

```bash
# Validate database schema
python main.py validate-schema

# Load processed data
python main.py load

# Run specific ETL stages
python main.py preprocess --transform --no-clean
```

---

## 🚀 Deployment

### Production Backend

The backend is currently deployed and accessible at:
- **API Base URL**: [http://54.93.51.85:8000](http://54.93.51.85:8000)
- **Health Check**: [http://54.93.51.85:8000/](http://54.93.51.85:8000/)
- **OpenAPI Docs**: [http://54.93.51.85:8000/docs](http://54.93.51.85:8000/docs)

### Local Development

```bash
# Using Docker Compose (if available)
docker-compose up --build

# Manual deployment
# Backend
cd backend/fastAPI
uvicorn main:app --host 0.0.0.0 --port 8000

# Frontend
cd frontend
npm run build
npm start
```

### Environment Variables

Required environment variables for production:

```bash
SUPABASE_URL=your_production_supabase_url
SUPABASE_SERVICE_KEY=your_service_key
SUPABASE_ANON_KEY=your_anon_key
NODE_ENV=production
```

---

## 📊 Data Sources

This project uses data from the **CORDIS** (Community Research and Development Information Service) database:

- **Projects**: Horizon Europe project information
- **Organizations**: Research institutions and companies
- **Publications**: Scientific publications from projects
- **Legal Basis**: Funding schemes and programs
- **Scientific Vocabulary**: Research field classifications

### Data Processing Pipeline

1. **Ingestion**: Download raw CSV files from CORDIS
2. **Cleaning**: Handle missing values and data inconsistencies
3. **Transformation**: Enrich data with geographic coordinates, topic modeling
4. **Loading**: Store processed data in Supabase PostgreSQL database

---

## 🤝 Contributing

We welcome contributions! Please follow these steps:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. **Push** to the branch (`git push origin feature/amazing-feature`)
5. **Open** a Pull Request

### Development Guidelines

- Follow **PEP 8** for Python code
- Use **ESLint** rules for TypeScript/React
- Add **tests** for new features
- Update **documentation** as needed
- Keep commits **atomic** and well-described

---

## 📋 Project Status

- ✅ **Backend API**: Fully functional with 20+ analytics endpoints
- ✅ **Database**: Complete ETL pipeline with Supabase integration
- ✅ **Frontend**: React components with interactive visualizations
- ✅ **Production Deployment**: Backend hosted and accessible
- 🚧 **Frontend Deployment**: In progress
- 🚧 **Advanced ML Models**: Topic modeling and predictions
- 📋 **Documentation**: Comprehensive API and user guides

---

## 📚 Resources and References

- [Horizon Europe Programme](https://ec.europa.eu/info/research-and-innovation/funding/funding-opportunities/funding-programmes-and-open-calls/horizon-europe_en)
- [CORDIS Database](https://cordis.europa.eu/)
- [CORDIS Datalab](https://cordis.europa.eu/datalab/datalab.php)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [React Documentation](https://react.dev/)
- [Supabase Documentation](https://supabase.com/docs)

---

## 📄 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

---

## 👥 Team

Developed by **MDA Group 20** as part of the EU Horizon research analysis project.

For questions, issues, or contributions, please open an issue on GitHub or contact the development team.
