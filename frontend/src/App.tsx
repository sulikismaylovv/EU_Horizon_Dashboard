import React, { useState, useEffect, useMemo } from 'react';
import Plot from 'react-plotly.js'; // Import Plotly
import './App.css';

// 1. Define the structure for a Project (mirroring your Supabase schema)
interface Project {
  project_id: string;
  name: string;
  start_year: number;
  country_code: string;
  budget: number;
  topics: string[];
  description: string;
}

// 2. Create dummy project data
const dummyProjects: Project[] = [
  {
    project_id: "proj_001",
    name: "Interactive Dashboard Development",
    start_year: 2024,
    country_code: "US",
    budget: 150000,
    topics: ["data visualization", "react", "supabase", "fastapi"],
    description: "Building an interactive dashboard with modern web technologies."
  },
  {
    project_id: "proj_002",
    name: "AI Powered Analytics Platform",
    start_year: 2023,
    country_code: "CA",
    budget: 275000,
    topics: ["machine learning", "big data", "python"],
    description: "An advanced analytics platform using AI to derive insights from large datasets."
  },
  {
    project_id: "proj_003",
    name: "E-commerce Site Upgrade",
    start_year: 2024,
    country_code: "GB",
    budget: 95000,
    topics: ["web development", "ux/ui", "payments", "react"],
    description: "Upgrading an existing e-commerce platform for better performance and user experience."
  },
  {
    project_id: "proj_004",
    name: "Mobile Health App",
    start_year: 2023,
    country_code: "DE",
    budget: 120000,
    topics: ["mobile dev", "health", "api integration", "python"],
    description: "A mobile application for tracking health metrics and providing personalized advice."
  },
  {
    project_id: "proj_005",
    name: "Cloud Migration Strategy",
    start_year: 2022,
    country_code: "US",
    budget: 220000,
    topics: ["cloud computing", "aws", "devops"],
    description: "Developing a strategy for migrating existing infrastructure to the cloud."
  },
  {
    project_id: "proj_006",
    name: "Sustainable Energy Research",
    start_year: 2023,
    country_code: "CA",
    budget: 180000,
    topics: ["sustainability", "research", "energy"],
    description: "Research project focused on developing new sustainable energy sources."
  }
];

// 3. Simulate fetching data
async function fetchMockProjects(): Promise<{ data: Project[] | null; error: any }> {
  return new Promise(resolve => {
    setTimeout(() => {
      resolve({ data: dummyProjects, error: null });
    }, 500); // Simulate network delay
  });
}

// --- Reusable Chart Components & Helper Functions ---

// Summary Card Component
interface SummaryCardProps {
  title: string;
  value: string | number;
  icon?: string; // Optional icon (e.g., emoji or class for an icon font)
}
const SummaryCard: React.FC<SummaryCardProps> = ({ title, value, icon }) => (
  <div className="summary-card">
    {icon && <div className="summary-card-icon">{icon}</div>}
    <div className="summary-card-value">{value}</div>
    <div className="summary-card-title">{title}</div>
  </div>
);

// --- Main App Component ---
function App() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadProjects = async () => {
      setLoading(true);
      setError(null);
      try {
        const { data, error: fetchError } = await fetchMockProjects();
        if (fetchError) throw new Error(typeof fetchError === 'string' ? fetchError : (fetchError as Error).message || "An unknown error occurred");
        setProjects(data || []);
      } catch (err) {
        setError((err as Error).message);
        setProjects([]);
      } finally {
        setLoading(false);
      }
    };
    loadProjects();
  }, []);

  // --- Data Processing for Charts (using useMemo for optimization) ---

  const projectsByYear = useMemo(() => {
    const counts: { [year: number]: number } = {};
    projects.forEach(p => {
      counts[p.start_year] = (counts[p.start_year] || 0) + 1;
    });
    const sortedYears = Object.keys(counts).map(Number).sort((a, b) => a - b);
    return {
      years: sortedYears,
      counts: sortedYears.map(year => counts[year]),
    };
  }, [projects]);

  const budgetByCountry = useMemo(() => {
    const budgets: { [country: string]: number } = {};
    projects.forEach(p => {
      budgets[p.country_code] = (budgets[p.country_code] || 0) + p.budget;
    });
    return {
      countries: Object.keys(budgets),
      budgets: Object.values(budgets),
    };
  }, [projects]);

  const topicsDistribution = useMemo(() => {
    const topicCounts: { [topic: string]: number } = {};
    projects.forEach(p => {
      p.topics.forEach(topic => {
        topicCounts[topic] = (topicCounts[topic] || 0) + 1;
      });
    });
    const sortedTopics = Object.entries(topicCounts).sort(([,a],[,b]) => b-a); // Sort by count desc
    return {
      topics: sortedTopics.map(([topic]) => topic),
      counts: sortedTopics.map(([,count]) => count),
    };
  }, [projects]);

  const totalBudget = useMemo(() => projects.reduce((sum, p) => sum + p.budget, 0), [projects]);
  const averageBudget = useMemo(() => (projects.length > 0 ? totalBudget / projects.length : 0), [projects, totalBudget]);

  // --- Render Logic ---
  if (loading) return <div className="app-loading">Loading Dashboard...</div>;
  if (error) return <div className="app-error">Error: {error}</div>;

  return (
    <div className="dashboard-container">
      <header className="dashboard-header">
        <h1>Project Analytics Dashboard</h1>
      </header>

      <section className="dashboard-summary-cards">
        <SummaryCard title="Total Projects" value={projects.length} icon="📊" />
        <SummaryCard title="Total Budget" value={`$${totalBudget.toLocaleString()}`} icon="💰" />
        <SummaryCard title="Avg. Budget" value={`$${averageBudget.toLocaleString(undefined, {maximumFractionDigits: 0})}`} icon="📈" />
        <SummaryCard title="Countries Involved" value={new Set(projects.map(p => p.country_code)).size} icon="🌍"/>
      </section>

      <section className="dashboard-charts">
        <div className="chart-card">
          <h2>Projects by Start Year</h2>
          <Plot
            data={[
              {
                x: projectsByYear.years,
                y: projectsByYear.counts,
                type: 'bar',
                marker: { color: '#1f77b4' },
              },
            ]}
            layout={{
              autosize: true,
              margin: { t: 30, b: 40, l: 40, r: 20 },
              xaxis: { title: { text: 'Year' } },
              yaxis: { title: {text:'Number of Projects' }},
            }}
            useResizeHandler={true}
            style={{ width: '100%', height: '100%' }}
          />
        </div>

        <div className="chart-card">
          <h2>Budget Distribution by Country</h2>
          <Plot
            data={[
              {
                labels: budgetByCountry.countries,
                values: budgetByCountry.budgets,
                type: 'pie',
                hoverinfo: 'label+percent+name',
                textinfo: 'label+percent',
                automargin: true,
              },
            ]}
            layout={{
              autosize: true,
              margin: { t: 30, b: 20, l: 20, r: 20 },
              legend: {orientation: 'h', y: -0.1}
            }}
            useResizeHandler={true}
            style={{ width: '100%', height: '100%' }}
          />
        </div>

        <div className="chart-card large-chart">
          <h2>Project Topics Distribution</h2>
           {topicsDistribution.topics.length > 0 ? (
            <Plot
              data={[
                {
                  x: topicsDistribution.counts,
                  y: topicsDistribution.topics,
                  type: 'bar',
                  orientation: 'h',
                  marker: { color: '#2ca02c' },
                },
              ]}
              layout={{
                autosize: true,
                margin: { t: 30, b: 40, l: 150, r: 20 }, // Increased left margin for topic labels
                xaxis: { title: {text:'Number of Projects'} },
                yaxis: { autorange: 'reversed' }, // Show most frequent on top
              }}
              useResizeHandler={true}
              style={{ width: '100%', height: '100%' }}
            />
          ) : <p>No topic data available.</p>}
        </div>

      </section>
      {/* You can add a section for the raw project list/table here if needed */}
      {/* <section className="dashboard-raw-data">
        <h2>Project Details</h2>
        ... display raw data table ...
      </section> */}
    </div>
  );
}

export default App;