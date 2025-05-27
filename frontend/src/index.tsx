import React, { useState, useEffect } from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import './dashboard.css';
import reportWebVitals from './reportWebVitals';
import Sunburst from './graphs/Sunburst';
import TopInstitutionsBarChart from './graphs/InstitutionsByFunding';
import ProjectsByCountryBarChart from './graphs/ProjectsByCountry';
import EcByCountryBarChart from './graphs/EcByCountry';
import FundingDistribution from './graphs/FundingDistribution';
import EfficiencyBubbleChart from './graphs/EfficiencyBubbleChart';
import ProgramDurationHeatmap from './graphs/ProgramDurationHeatmap';
import ParticipationTrends from './graphs/ParticipationTrends';
import ProjectTimeline from './graphs/ProjectTimeline';
import ImpactAnalysis from './graphs/ImpactAnalysis';

import TopicEvolution from './components/TopicEvolution';
import ProjectSeasonality from './components/ProjectSeasonality';
// import ResearchFieldNetwork from './components/ResearchFieldNetwork';
// import CollaborationNetwork from './components/CollaborationNetwork';
// import ResearchOutputTimeline from './components/ResearchOutputTimeline';
import InteractiveMap from './components/InteractiveMap';


  const Dashboard = () => {
    const [category, setCategory] = useState<'research'|'program'|'monitoring'|'map'>('research');
    const [expandedChart, setExpandedChart] = useState<string | null>(null);

    // Handle ESC key to close modal
    useEffect(() => {
      const handleKeyPress = (e: KeyboardEvent) => {
        if (e.key === 'Escape' && expandedChart) {
          handleCloseExpanded();
        }
      };

      if (expandedChart) {
        document.addEventListener('keydown', handleKeyPress);
        document.body.style.overflow = 'hidden'; // Prevent background scrolling
      }

      return () => {
        document.removeEventListener('keydown', handleKeyPress);
        document.body.style.overflow = 'auto'; // Restore scrolling
      };
    }, [expandedChart]);

    const handleExpandChart = (chartId: string) => {
      setExpandedChart(chartId);
    };

    const handleCloseExpanded = () => {
      setExpandedChart(null);
    };

    const handleOverlayClick = (e: React.MouseEvent) => {
      if (e.target === e.currentTarget) {
        handleCloseExpanded();
      }
    };

    const getChartTitle = (chartId: string): string => {
      const chartTitles: Record<string, string> = {
        'research-field-distribution': 'Research Field Distribution',
        'top-institutions-funding': 'Top Institutions by Funding',
        'projects-by-country': 'Projects by Country',
        'ec-contribution-country': 'EC Contribution by Country',
        'research-field-network': 'Research Field Network',
        'collaboration-network': 'Impact Analysis',
        'research-output-timeline': 'Research Output Timeline',
        'funding-distribution': 'Funding Distribution',
        'efficiency-analysis': 'Efficiency Analysis',
        'program-duration': 'Program Duration',
        'project-seasonality': 'Project Seasonality',
        'topic-evolution': 'Topic Evolution',
        'participation-trends': 'Participation Trends',
        'project-timeline': 'Project Timeline',
        'interactive-map': 'EU Horizon Projects Interactive Map'
      };
      return chartTitles[chartId] || 'Chart';
    };

    const renderExpandedChart = (chartId: string): React.ReactNode => {
      switch (chartId) {
        case 'research-field-distribution':
          return <Sunburst />;
        case 'top-institutions-funding':
          return <TopInstitutionsBarChart />;
        case 'projects-by-country':
          return <ProjectsByCountryBarChart />;
        case 'ec-contribution-country':
          return <EcByCountryBarChart />;
        case 'collaboration-network':
          return <ImpactAnalysis />;
        case 'funding-distribution':
          return <FundingDistribution />;
        case 'efficiency-analysis':
          return <EfficiencyBubbleChart />;
        case 'program-duration':
          return <ProgramDurationHeatmap />;
        case 'project-seasonality':
          return <ProjectSeasonality />;
        case 'topic-evolution':
          return <TopicEvolution />;
        case 'participation-trends':
          return <ParticipationTrends />;
        case 'project-timeline':
          return <ProjectTimeline />;
        case 'interactive-map':
          return <InteractiveMap />;
        default:
          return null;
      }
    };

    const ChartCard = ({ id, title, children, fullWidth = false }: { id: string; title: string; children: React.ReactNode; fullWidth?: boolean }) => (
      <div className={`chart-card ${fullWidth ? 'full-width' : ''}`}>
        <div className="chart-header">
          <h3 className="chart-title">{title}</h3>
          <div className="chart-menu">
            <button title="Expand" onClick={() => handleExpandChart(id)}>⛶</button>
            <button title="Download">⬇</button>
          </div>
        </div>
        <div className="chart-content">
          {children}
        </div>
      </div>
    );

    const renderCategoryContent = () => {
      switch (category) {
        case 'research':
          return (
            <>
              <div className="charts-container">
                <ChartCard id="research-field-distribution" title="Research Field Distribution">
                  <Sunburst />
                </ChartCard>

                <ChartCard id="top-institutions-funding" title="Top Institutions by Funding">
                  <TopInstitutionsBarChart />
                </ChartCard>

                <ChartCard id="projects-by-country" title="Projects by Country">
                  <ProjectsByCountryBarChart />
                </ChartCard>

                <ChartCard id="ec-contribution-country" title="EC Contribution by Country">
                  <EcByCountryBarChart />
                </ChartCard>

                <ChartCard id="collaboration-network" title="Impact Analysis" fullWidth>
                  <ImpactAnalysis />
                </ChartCard>



              </div>
            </>
          );

        case 'program':
          return (
            <>
              <div className="charts-container">
                <ChartCard id="funding-distribution" title="Funding Distribution">
                  <FundingDistribution />
                </ChartCard>

                <ChartCard id="efficiency-analysis" title="Efficiency Analysis">
                  <EfficiencyBubbleChart />
                </ChartCard>

                <ChartCard id="program-duration" title="Program Duration">
                  <ProgramDurationHeatmap />
                </ChartCard>

                <ChartCard id="project-seasonality" title="Project Seasonality">
                  <ProjectSeasonality />
                </ChartCard>

                <ChartCard id="topic-evolution" title="Topic Evolution" fullWidth>
                  <TopicEvolution />
                </ChartCard>
              </div>
            </>
          );

        case 'monitoring':
          return (
            <>
              <div className="charts-container">
                <ChartCard id="participation-trends" title="Participation Trends">
                  <ParticipationTrends />
                </ChartCard>

                <ChartCard id="project-timeline" title="Project Timeline" fullWidth>
                  <ProjectTimeline />
                </ChartCard>
              </div>
            </>
          );

        case 'map':
          return (
            <>
              <div className="charts-container">
                <div className="chart-card interactive-map">
                  <div className="chart-header">
                    <h3 className="chart-title">EU Horizon Projects Interactive Map</h3>
                    <div className="chart-menu">
                      <button title="Expand" onClick={() => handleExpandChart('interactive-map')}>⛶</button>
                      <button title="Download">⬇</button>
                    </div>
                  </div>
                  <div className="chart-content">
                    <div className="map-placeholder">
                      <div className="map-placeholder-content">
                        <div className="map-placeholder-icon">🗺️</div>
                        <h4>Interactive Map View</h4>
                        <p>Click the expand button (⛶) above to load the interactive map showing EU Horizon projects across Europe.</p>
                        <button 
                          className="map-placeholder-button"
                          onClick={() => handleExpandChart('interactive-map')}
                        >
                          Open Interactive Map
                        </button>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </>
          );

        default:
          return null;
      }
    };

    return (
      <div className="dashboard-container">
        {/* Header */}
        <header className="dashboard-header">
          <div className="header-content">
            <div className="logo-section">
              <div className="logo">EU</div>
              <div>
                <h1 className="dashboard-title">Horizon Europe Dashboard</h1>
                <p className="dashboard-subtitle">Interactive Research Funding Analytics</p>
              </div>
            </div>

            <nav className="nav-buttons">
              <button 
                className={`nav-button ${category === 'research' ? 'active' : ''}`}
                onClick={() => setCategory('research')}
              >
                Research Analytics
              </button>
              <button 
                className={`nav-button ${category === 'program' ? 'active' : ''}`}
                onClick={() => setCategory('program')}
              >
                Program Insights
              </button>
              <button 
                className={`nav-button ${category === 'monitoring' ? 'active' : ''}`}
                onClick={() => setCategory('monitoring')}
              >
                Performance Monitoring
              </button>
              <button 
                className={`nav-button ${category === 'map' ? 'active' : ''}`}
                onClick={() => setCategory('map')}
              >
                Interactive Map
              </button>
            </nav>

            {/* <div className="action-buttons">
              <button 
                className="action-button"
                onClick={() => setFiltersVisible(!filtersVisible)}
              >
                🔍 Filters
              </button>
              <button className="action-button">
                📊 Export
              </button>
              <button className="action-button primary">
                📈 Generate Report
              </button>
            </div> */}
          </div>
        </header>

        {/* Main Content */}
        <main className="dashboard-main">
          {/* Stats Overview */}
          <section className="stats-overview">
            <div className="stat-card">
              <div className="stat-value">€21.5B</div>
              <div className="stat-label">Total EC Funding</div>
              <div className="stat-change negative">-25.0% from last year</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">14,820</div>
              <div className="stat-label">Active Projects</div>
              <div className="stat-change negative">-3.3% from last year</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">101,076</div>
              <div className="stat-label">Research Organizations</div>
              <div className="stat-change positive">Participating entities</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">26</div>
              <div className="stat-label">EU Countries</div>
              <div className="stat-change">Member states participating</div>
            </div>
            <div className="stat-card">
              <div className="stat-value">24,150</div>
              <div className="stat-label">Publications</div>
              <div className="stat-change positive">Research outputs</div>
            </div>
          </section>



          {/* Charts Content */}
          {renderCategoryContent()}
        </main>

        {/* Modal Overlay for Expanded Charts */}
        {expandedChart && (
          <div className="chart-modal-overlay" onClick={handleOverlayClick}>
            <div className="chart-modal">
              <div className="chart-modal-header">
                <h2 className="chart-modal-title">
                  {getChartTitle(expandedChart)}
                </h2>
                <button 
                  className="chart-modal-close"
                  onClick={handleCloseExpanded}
                  title="Close"
                >
                  ✕
                </button>
              </div>
              <div className="chart-modal-content">
                {renderExpandedChart(expandedChart)}
              </div>
            </div>
          </div>
        )}

        {/* Footer */}
        <footer className="footer">
          <p>© 2024 EU Horizon Dashboard. Data sourced from CORDIS Database. Last updated: {new Date().toLocaleDateString()}</p>
        </footer>
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
