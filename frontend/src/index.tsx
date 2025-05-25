import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import reportWebVitals from './reportWebVitals';
import Sunburst from './graphs/sunburst'; // Importing the sunburst component
import TopInstitutionsBarChart from './graphs/InstitutionsByFunding';
import ProjectsByCountryBarChart from './graphs/ProjectsByCountry'; // Importing the projects by country bar chart
import EcByCountryBarChart from './graphs/EcByCountry';

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);
root.render(
  <React.StrictMode>
    <EcByCountryBarChart />
    <TopInstitutionsBarChart />
    <ProjectsByCountryBarChart />
    <Sunburst />
  </React.StrictMode>
);

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals();
