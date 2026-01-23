"""
PlumberAI Flask Application Entry Point

This is the main entry point for the Flask web server. It imports the 
configured app object from handle_incoming_call and runs the server.

Usage:
    python3 execution/run_app.py
    OR
    pm2 start execution/run_app.py --interpreter python3

Environment Variables:
    - PORT: Server port (default: 5002)
    - HOST: Server host (default: 127.0.0.1 for security)
"""

import os
import sys

# Fix PYTHONPATH for PM2 - find project root and add to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)  # Go up from execution/
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from execution.handle_incoming_call import app

if __name__ == "__main__":
    port = int(os.getenv('PORT', 5002))
    host = os.getenv('HOST', '127.0.0.1')  # Bind to localhost only (nginx handles public)
    
    # Run in debug mode only if explicitly enabled
    debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    
    print(f"🚀 Starting PlumberAI Server on {host}:{port}")
    app.run(host=host, port=port, debug=debug)
