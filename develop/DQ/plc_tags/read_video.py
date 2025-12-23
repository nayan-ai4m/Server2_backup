import dash
from dash import html, dcc
from dash.dependencies import Input, Output, State
import subprocess
import signal
from collections import defaultdict
import os

# Store processes
camera_processes = defaultdict(lambda: None)

app = dash.Dash(__name__)

# Camera configurations
cameras = [
    {"name": "Machines 17 & 18", "ip": "192.168.1.21", "id": "192_168_1_21"},
    {"name": "Machines 19 & 20", "ip": "192.168.1.20", "id": "192_168_1_20"},
    {"name": "Machines 21 & 22", "ip": "192.168.1.22", "id": "192_168_1_22"},
    {"name": "Machines 25 & 26", "ip": "192.168.1.27", "id": "192_168_1_27"},
    {"name": "Machines 27 & 28", "ip": "192.168.1.25", "id": "192_168_1_25"},
    {"name": "Machines 29 & 30", "ip": "192.168.1.23", "id": "192_168_1_23"},
    {"name": "Loop 3 Taping", "ip": "192.168.1.24", "id": "192_168_1_24"},
    {"name": "Loop 4 Taping", "ip": "192.168.1.26", "id": "192_168_1_26"},
    {"name": "EOL Loop 3", "ip": "192.168.1.29", "id": "192_168_1_29"},
    {"name": "EOL Loop 4", "ip": "192.168.1.28", "id": "192_168_1_28"}
]

# Button styles
button_style = {
    'start': {
        'backgroundColor': 'green',
        'color': 'white',
        'margin': '10px',
        'padding': '15px 32px',  # Increased padding
        'fontSize': '16px',      # Larger font
        'borderRadius': '8px',   # Rounded corners
        'border': 'none',
        'cursor': 'pointer',
        'width': '120px',        # Fixed width
        'height': '50px'         # Fixed height
    },
    'stop': {
        'backgroundColor': 'red',
        'color': 'white',
        'margin': '10px',
        'padding': '15px 32px',  # Increased padding
        'fontSize': '16px',      # Larger font
        'borderRadius': '8px',   # Rounded corners
        'border': 'none',
        'cursor': 'pointer',
        'width': '120px',        # Fixed width
        'height': '50px'         # Fixed height
    }
}

def create_gstreamer_command(ip):
    return [
        'gst-launch-1.0',
        f'rtspsrc location=rtsp://admin:unilever2024@{ip}:554/Streaming/Channels/101',
        '!',
        'rtph265depay',
        '!',
        'h265parse',
        '!',
        'splitmuxsink',
        'muxer=qtmux',
        f'location=camera{ip.split(".")[-1]}/test%03d.mp4',
        'max-size-time=300000000000'
    ]

app.layout = html.Div([
    html.H1("CCTV Camera Control Panel", style={'textAlign': 'center'}),
    html.Div([
        html.Div([
            html.H3(camera['name']),
            html.Div([
                html.Button('Start', id=f'start-{camera["id"]}', n_clicks=0,
                           style=button_style['start']),
                html.Button('Stop', id=f'stop-{camera["id"]}', n_clicks=0,
                           style=button_style['stop']),
                html.Div(id=f'status-{camera["id"]}', style={
                    'marginLeft': '20px',
                    'fontSize': '16px',
                    'width': '150px'
                })
            ], style={'display': 'flex', 'alignItems': 'center'})
        ], style={
            'border': '1px solid #ddd',
            'padding': '20px',
            'margin': '10px',
            'borderRadius': '5px',
            'backgroundColor': '#f9f9f9',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'  # Added shadow for depth
        }) for camera in cameras
    ])
])

def create_callback(camera):
    @app.callback(
        Output(f'status-{camera["id"]}', 'children'),
        [Input(f'start-{camera["id"]}', 'n_clicks'),
         Input(f'stop-{camera["id"]}', 'n_clicks')],
        [State(f'status-{camera["id"]}', 'children')]
    )
    def control_camera(start_clicks, stop_clicks, current_status):
        ctx = dash.callback_context
        if not ctx.triggered:
            return "Not Started"
        
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        if button_id == f'start-{camera["id"]}' and start_clicks > 0:
            if camera_processes[camera["ip"]] is None:
                try:
                    os.makedirs(f'camera{camera["ip"].split(".")[-1]}', exist_ok=True)
                    cmd = create_gstreamer_command(camera["ip"])
                    process = subprocess.Popen(cmd)
                    camera_processes[camera["ip"]] = process
                    return "Recording"
                except Exception as e:
                    return f"Error: {str(e)}"
            return "Already Recording"
            
        elif button_id == f'stop-{camera["id"]}' and stop_clicks > 0:
            if camera_processes[camera["ip"]] is not None:
                try:
                    camera_processes[camera["ip"]].send_signal(signal.SIGINT)
                    camera_processes[camera["ip"]] = None
                    return "Stopped"
                except Exception as e:
                    return f"Error stopping: {str(e)}"
            return "Not Recording"
            
        return current_status or "Not Started"

# Create callbacks for each camera
for camera in cameras:
    create_callback(camera)

if __name__ == '__main__':
    app.run_server(debug=True, port=8050)
