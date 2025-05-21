import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

"""
Main entry point of the Dash application, defining the layout structure, initializing the server, and running the application.
"""


# Initialize the Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

# Define the app layout
app.layout = html.Div([
    html.H1("EU Horizon Dashboard"),
    # Add more components here
])

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)