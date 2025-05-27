import React, { useState, useEffect, useMemo } from 'react';
import Plot from 'react-plotly.js';

interface MapOrganizationData {
  id: number;
  name: string;
  country: string;
  iso_alpha_3: string | null;
  latitude: number | null;
  longitude: number | null;
  activity_type: string | null;
  role: string | null;
  ec_contribution: number | null;
}

interface MapProjectData {
  id: number;
  acronym: string | null;
  title: string | null;
  ec_max_contribution: number | null;
  start_year: number | null;
  funding_scheme: string | null;
  field_class: string | null;
  field: string | null;
  sub_field: string | null;
  niche: string | null;
  coordinator_name: string | null;
}

interface CountrySummaryData {
  country: string;
  iso_alpha_3: string | null;
  latitude: number | null;
  longitude: number | null;
  total_contribution: number;
  project_count: number;
  log_contribution: number;
  euros_per_100k_inhabitants: number | null;
  log_contribution_per_100k: number | null;
}

interface CollaborationEdge {
  org1_name: string;
  org2_name: string;
  org1_lat: number | null;
  org1_lon: number | null;
  org2_lat: number | null;
  org2_lon: number | null;
  project_id: number;
  project_acronym: string | null;
  project_title: string | null;
  coordinator_name: string | null;
}

interface InteractiveMapResponse {
  country_summary: CountrySummaryData[];
  organizations: MapOrganizationData[];
  projects: MapProjectData[];
  collaboration_edges: CollaborationEdge[];
  table_data: Array<{
    project_acronym: string;
    ec_max_contribution: number;
    title: string;
    institutes: string;
  }>;
  available_filters: {
    countries: string[];
    funding_schemes: string[];
    start_years: string[];
    activity_types: string[];
    roles: string[];
  };
}

interface MapFilters {
  country: string | null;
  funding_scheme: string | null;
  start_year: string | null;
  field_class: string | null;
  field: string | null;
  sub_field: string | null;
  niche: string | null;
  activity_type: string | null;
  role: string | null;
}

const InteractiveMap: React.FC = () => {
  const [data, setData] = useState<InteractiveMapResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filters, setFilters] = useState<MapFilters>({
    country: null,
    funding_scheme: null,
    start_year: null,
    field_class: null,
    field: null,
    sub_field: null,
    niche: null,
    activity_type: null,
    role: null,
  });
  const [showPerCapita, setShowPerCapita] = useState(false);
  const [showNetwork, setShowNetwork] = useState(false);
  const [showOrgPins, setShowOrgPins] = useState(true);
  const [selectedCountry, setSelectedCountry] = useState<string>('all');

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        console.log('Fetching interactive map data...');
        
        // Build query parameters
        const params = new URLSearchParams();
        Object.entries(filters).forEach(([key, value]) => {
          if (value !== null && value !== '') {
            params.append(key, value.toString());
          }
        });
        const endpoint = process.env.NODE_ENV === 'development'
          ? '/analytics/interactive-map' // CRA will proxy this to http://localhost:8000
          : '/api/analytics/interactive-map'; // Vercel will rewrite this to your catch-all
        const response = await fetch(`${endpoint}?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const result = await response.json();
        console.log('Interactive map data received:', result);
        setData(result);
      } catch (err) {
        console.error('Error fetching interactive map data:', err);
        setError(err instanceof Error ? err.message : 'An error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [filters]);

  const plotData = useMemo(() => {
    if (!data) return [];

    const traces: any[] = [];

    if (selectedCountry === 'all') {
      // Country bubble chart
      const colorCol = showPerCapita ? 'log_contribution_per_100k' : 'log_contribution';
      const countrySummary = data.country_summary.filter(c => c.latitude && c.longitude);
      
      if (countrySummary.length > 0) {
        traces.push({
          type: 'scattermapbox',
          lat: countrySummary.map(c => c.latitude),
          lon: countrySummary.map(c => c.longitude),
          mode: 'markers',
          marker: {
            size: countrySummary.map(c => Math.sqrt(c.total_contribution) / 1000),
            color: showPerCapita 
              ? countrySummary.map(c => c.log_contribution_per_100k || 0)
              : countrySummary.map(c => c.log_contribution),
            colorscale: 'Viridis',
            showscale: true,
            colorbar: {
              title: showPerCapita ? 'Log(€/100k inhabitants)' : 'Log(Total Contribution)',
            },
            sizemin: 8,
            sizemax: 50,
          },
          text: countrySummary.map(c => 
            `Country: ${c.country}<br>` +
            `Projects: ${c.project_count}<br>` +
            `Total Contribution: €${c.total_contribution.toLocaleString()}<br>` +
            (c.euros_per_100k_inhabitants ? `€/100k inhabitants: €${c.euros_per_100k_inhabitants.toFixed(2)}` : '')
          ),
          hovertemplate: '%{text}<extra></extra>',
          name: 'Countries',
        });
      }

      // Organization pins
      if (showOrgPins) {
        const orgsWithCoords = data.organizations.filter(org => org.latitude && org.longitude);
        if (orgsWithCoords.length > 0) {
          traces.push({
            type: 'scattermapbox',
            lat: orgsWithCoords.map(org => org.latitude),
            lon: orgsWithCoords.map(org => org.longitude),
            mode: 'markers',
            marker: {
              size: 6,
              color: 'green',
              opacity: 0.4,
            },
            text: orgsWithCoords.map(org => org.name),
            hovertemplate: '%{text}<extra></extra>',
            name: 'Organizations',
          });
        }
      }
    } else {
      // Country-specific view - density map
      const countryOrgs = data.organizations.filter(org => 
        org.country === selectedCountry && org.latitude && org.longitude
      );
      
      if (countryOrgs.length > 0) {
        traces.push({
          type: 'densitymapbox',
          lat: countryOrgs.map(org => org.latitude),
          lon: countryOrgs.map(org => org.longitude),
          z: countryOrgs.map(org => org.ec_contribution || 1),
          radius: 15,
          colorscale: 'Viridis',
          showscale: true,
          colorbar: {
            title: 'EC Contribution'
          },
          name: 'Contribution Density',
        });
      }

      // Organization pins for selected country
      if (showOrgPins && countryOrgs.length > 0) {
        traces.push({
          type: 'scattermapbox',
          lat: countryOrgs.map(org => org.latitude),
          lon: countryOrgs.map(org => org.longitude),
          mode: 'markers',
          marker: {
            size: 6,
            color: 'green',
            opacity: 0.4,
          },
          text: countryOrgs.map(org => org.name),
          hovertemplate: '%{text}<extra></extra>',
          name: 'Organizations',
        });
      }
    }

    // Collaboration network
    if (showNetwork) {
      data.collaboration_edges.forEach(edge => {
        if (edge.org1_lat && edge.org1_lon && edge.org2_lat && edge.org2_lon) {
          traces.push({
            type: 'scattermapbox',
            lat: [edge.org1_lat, edge.org2_lat],
            lon: [edge.org1_lon, edge.org2_lon],
            mode: 'lines',
            line: {
              width: 2,
              color: 'blue',
            },
            opacity: 0.4,
            hovertemplate: 
              `<b>Project:</b> ${edge.project_acronym || 'N/A'}<br>` +
              `<b>Title:</b> ${edge.project_title || 'N/A'}<br>` +
              `<b>Coordinator:</b> ${edge.coordinator_name || 'N/A'}<extra></extra>`,
            showlegend: false,
          });
        }
      });
    }

    return traces;
  }, [data, showPerCapita, showNetwork, showOrgPins, selectedCountry]);

  const layout = useMemo(() => {
    const center = selectedCountry === 'all' 
      ? { lat: 54, lon: 15 } // Europe center
      : data?.country_summary.find(c => c.country === selectedCountry) 
        ? { 
            lat: data.country_summary.find(c => c.country === selectedCountry)!.latitude || 54, 
            lon: data.country_summary.find(c => c.country === selectedCountry)!.longitude || 15 
          }
        : { lat: 54, lon: 15 };

    return {
      mapbox: {
        style: 'open-street-map',
        center: center,
        zoom: selectedCountry === 'all' ? 2 : 5,
      },
      autosize: true,
      margin: { l: 0, r: 0, t: 30, b: 0 },
      height: 600,
      title: {
        text: selectedCountry === 'all' 
          ? 'EU Horizon Projects - Global View' 
          : `EU Horizon Projects - ${selectedCountry}`,
        x: 0.5,
      },
    };
  }, [selectedCountry, data]);

  const handleFilterChange = (key: keyof MapFilters, value: any) => {
    setFilters(prev => ({ ...prev, [key]: value }));
  };

  const resetFilters = () => {
    setFilters({
      country: null,
      funding_scheme: null,
      start_year: null,
      field_class: null,
      field: null,
      sub_field: null,
      niche: null,
      activity_type: null,
      role: null,
    });
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-lg">Loading interactive map...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded">
        <strong>Error:</strong> {error}
      </div>
    );
  }

  if (!data) {
    return (
      <div className="text-center py-8">
        <p>No data available</p>
      </div>
    );
  }

  return (
    <div className="w-full space-y-4">
      {/* Filter Controls */}
      <div className="bg-gray-50 p-4 rounded-lg">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {/* First Column */}
          <div className="space-y-3">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Country:
              </label>
              <select
                value={selectedCountry}
                onChange={(e) => setSelectedCountry(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="all">All Countries</option>
                {data.available_filters.countries.map(country => (
                  <option key={country} value={country}>{country}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Funding Scheme:
              </label>
              <select
                value={filters.funding_scheme || ''}
                onChange={(e) => handleFilterChange('funding_scheme', e.target.value || null)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="">All</option>
                {data.available_filters.funding_schemes.map(scheme => (
                  <option key={scheme} value={scheme}>{scheme}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Start Year:
              </label>
              <select
                value={filters.start_year || ''}
                onChange={(e) => handleFilterChange('start_year', e.target.value || null)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="">All</option>
                {data.available_filters.start_years.map(year => (
                  <option key={year} value={year}>{year}</option>
                ))}
              </select>
            </div>
          </div>

          {/* Second Column */}
          <div className="space-y-3">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Activity Type:
              </label>
              <select
                value={filters.activity_type || ''}
                onChange={(e) => handleFilterChange('activity_type', e.target.value || null)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="">All</option>
                {data.available_filters.activity_types.map(type => (
                  <option key={type} value={type}>{type}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Organization Role:
              </label>
              <select
                value={filters.role || ''}
                onChange={(e) => handleFilterChange('role', e.target.value || null)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="">All</option>
                {data.available_filters.roles.map(role => (
                  <option key={role} value={role}>{role}</option>
                ))}
              </select>
            </div>

            <button
              onClick={resetFilters}
              className="w-full px-4 py-2 bg-gray-600 text-white rounded-md hover:bg-gray-700 focus:outline-none focus:ring-2 focus:ring-gray-500"
            >
              Reset Filters
            </button>
          </div>

          {/* Third Column */}
          <div className="space-y-3">
            <div>
              <label className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  checked={showPerCapita}
                  onChange={(e) => setShowPerCapita(e.target.checked)}
                  className="rounded"
                />
                <span className="text-sm font-medium text-gray-700">
                  Show per 100k inhabitants
                </span>
              </label>
            </div>

            <div>
              <label className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  checked={showNetwork}
                  onChange={(e) => setShowNetwork(e.target.checked)}
                  className="rounded"
                />
                <span className="text-sm font-medium text-gray-700">
                  Show collaboration network
                </span>
              </label>
            </div>

            <div>
              <label className="flex items-center space-x-2">
                <input
                  type="checkbox"
                  checked={showOrgPins}
                  onChange={(e) => setShowOrgPins(e.target.checked)}
                  className="rounded"
                />
                <span className="text-sm font-medium text-gray-700">
                  Show organization pins
                </span>
              </label>
            </div>
          </div>
        </div>
      </div>

      {/* Map */}
      <Plot
        data={plotData}
        layout={layout}
        config={{
          scrollZoom: true,
          displayModeBar: true,
          responsive: false,
        }}
        style={{ width: '100%', height: '600px' }}
      />

      {/* Project Table */}
      <div className="bg-white rounded-lg shadow-sm border">
        <div className="px-4 py-3 border-b">
          <h3 className="text-lg font-semibold text-gray-800">
            Projects shown on the map ({data.table_data.length})
          </h3>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Acronym
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  EC Contribution
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Title
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Institutes
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {data.table_data.slice(0, 10).map((project, index) => (
                <tr key={index} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {project.project_acronym}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    €{project.ec_max_contribution.toLocaleString()}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-500">
                    <div className="max-w-xs truncate" title={project.title}>
                      {project.title}
                    </div>
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-500">
                    <div className="max-w-xs truncate" title={project.institutes}>
                      {project.institutes}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {data.table_data.length > 10 && (
            <div className="px-6 py-3 text-sm text-gray-500 text-center border-t">
              Showing 10 of {data.table_data.length} projects
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default InteractiveMap;
