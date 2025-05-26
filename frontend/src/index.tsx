import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import reportWebVitals from './reportWebVitals';
import Sunburst from './graphs/sunburst';
import TopInstitutionsBarChart from './graphs/InstitutionsByFunding';
import ProjectsByCountryBarChart from './graphs/ProjectsByCountry';
import EcByCountryBarChart from './graphs/EcByCountry';
import FundingDistribution from './graphs/FundingDistribution';
import EfficiencyBubbleChart from './graphs/EfficiencyBubbleChart';
import ProgramDurationHeatmap from './graphs/ProgramDurationHeatmap';
import ParticipationTrends from './graphs/ParticipationTrends';
import ProjectTimeline from './graphs/ProjectTimeline';

import TopicEvolution from './components/TopicEvolution';
import ProjectSeasonality from './components/ProjectSeasonality';
import ResearchFieldNetwork from './components/ResearchFieldNetwork';
import ProjectSuccessAnalysis from './components/ProjectSuccessAnalysis';
import ResearchOutputTimeline from './components/ResearchOutputTimeline';


const Dashboard = () => {
  return (
    <div className="dashboard">
      <h1>EU Projects Analytics Dashboard</h1>
      <div className="graphs">

        <TopicEvolution />
        <ProjectSeasonality />
        <ResearchFieldNetwork />
        <ProjectSuccessAnalysis />
        <ResearchOutputTimeline />

        jdkasjdklas
        <Sunburst />
        <TopInstitutionsBarChart />
        <ProjectsByCountryBarChart />
        <EcByCountryBarChart />
        <FundingDistribution />
        <EfficiencyBubbleChart />
        <ProgramDurationHeatmap />
        <ParticipationTrends />
        <ProjectTimeline />
      </div>
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
