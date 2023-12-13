import plotly.graph_objects as go

# Data
models = ['Random Forest', 'Gradient Boosted Trees']
mae_values = [64.299, 59.719]
rmse_values = [231.188, 245.675]
r_squared_values = [0.727, 0.691]

# Create a table
table_fig = go.Figure(data=[go.Table(
    header=dict(values=['Model', 'MAE', 'RMSE', 'R-squared']),
    cells=dict(values=[models, mae_values, rmse_values, r_squared_values],
               fill=dict(color=[['rgba(255, 0, 0, 0.2)' if val == 'Random Forest' else 'rgba(255, 255, 255, 0)' for val in models],
                                ['rgba(255, 0, 0, 0.2)' if val == 0.727 else 'rgba(255, 255, 255, 0)' for val in r_squared_values]])))
])

# Update layout with colored title
table_fig.update_layout(
    height=400,
    width=600,
    title=dict(text='Model Comparison - Performance Metrics', font=dict(size=20)),
    title_x=0.5  # Center the title
)

# Save the table figure to HTML
table_fig.write_html('C:/Users/fsx87/Desktop/CV/table.html')

