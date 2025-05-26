import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import reportWebVitals from './reportWebVitals';
import Sunburst from './graphs/sunburst';
import TopInstitutionsBarChart from './graphs/InstitutionsByFunding';
import ProjectsByCountryBarChart from './graphs/ProjectsByCountry';
import EcByCountryBarChart from './graphs/EcByCountry';

const Dashboard = () => {
  return (
    <div className="dashboard">
      {/* Header */}
      <header className="dashboard-header">
        <div className="header-content">
          <div className="logo-section">
            <div className="eu-flag">🇪🇺</div>
            <h1>EU Horizon Dashboard</h1>
          </div>
          <div className="header-stats">
            <span className="last-updated">Last updated: {new Date().toLocaleDateString()}</span>
          </div>
        </div>
      </header>

      {/* Sidebar Navigation */}
      <aside className="sidebar">
        <nav className="nav-menu">
          <div className="nav-item active">
            <span className="nav-icon">📊</span>
            <span>Overview</span>
          </div>
          <div className="nav-item">
            <span className="nav-icon">🏛️</span>
            <span>Institutions</span>
          </div>
          <div className="nav-item">
            <span className="nav-icon">🌍</span>
            <span>Countries</span>
          </div>
          <div className="nav-item">
            <span className="nav-icon">💰</span>
            <span>Funding</span>
          </div>
          <div className="nav-item">
            <span className="nav-icon">📈</span>
            <span>Analytics</span>
          </div>
        </nav>
      </aside>

      {/* Main Content */}
      <main className="main-content">
        {/* Key Metrics Cards */}
        <section className="metrics-section">
          <div className="metric-card">
            <div className="metric-icon">💰</div>
            <div className="metric-content">
              <h3>Total Funding</h3>
              <p className="metric-value">€95.5B</p>
              <span className="metric-change positive">+12.3%</span>
            </div>
          </div>
          <div className="metric-card">
            <div className="metric-icon">🏛️</div>
            <div className="metric-content">
              <h3>Active Institutions</h3>
              <p className="metric-value">15,432</p>
              <span className="metric-change positive">+5.7%</span>
            </div>
          </div>
          <div className="metric-card">
            <div className="metric-icon">📋</div>
            <div className="metric-content">
              <h3>Total Projects</h3>
              <p className="metric-value">28,965</p>
              <span className="metric-change positive">+8.2%</span>
            </div>
          </div>
          <div className="metric-card">
            <div className="metric-icon">🌍</div>
            <div className="metric-content">
              <h3>Countries</h3>
              <p className="metric-value">27</p>
              <span className="metric-change neutral">0%</span>
            </div>
          </div>
        </section>

        {/* Charts Grid */}
        <section className="charts-section">
          <div className="chart-container large">
            <div className="chart-header">
              <h2>EC Contribution by Country</h2>
              <div className="chart-controls">
                <button className="btn-secondary">Export</button>
                <button className="btn-primary">Filter</button>
              </div>
            </div>
            <EcByCountryBarChart />
          </div>

          <div className="chart-container medium">
            <div className="chart-header">
              <h2>Top Institutions by Funding</h2>
              <div className="chart-controls">
                <button className="btn-secondary">Export</button>
              </div>
            </div>
            <TopInstitutionsBarChart />
          </div>

          <div className="chart-container medium">
            <div className="chart-header">
              <h2>Projects Distribution</h2>
              <div className="chart-controls">
                <button className="btn-secondary">Export</button>
              </div>
            </div>
            <Sunburst />
          </div>

          <div className="chart-container large">
            <div className="chart-header">
              <h2>Projects by Country</h2>
              <div className="chart-controls">
                <button className="btn-secondary">Export</button>
                <button className="btn-primary">Filter</button>
              </div>
            </div>
            <ProjectsByCountryBarChart />
          </div>

          <div className="chart-container medium">
            <div className="chart-header">
              <h2>Funding Distribution</h2>
              <div className="chart-controls">
                <button className="btn-secondary">Export</button>
              </div>
            </div>
            <Sunburst />
          </div>

          <div className="chart-container medium">
            <div className="chart-header">
              <h2>Regional Analysis</h2>
              <div className="chart-controls">
                <button className="btn-secondary">Export</button>
              </div>
            </div>
            <EcByCountryBarChart />
          </div>
        </section>
      </main>
    </div>
  );
};

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);
root.render(
  <React.StrictMode>
    <Dashboard />
  </React.StrictMode>
);

reportWebVitals();
