#!/bin/bash
# Start Flask in background
python app.py &
FLASK_PID=$!
echo "Flask started with PID $FLASK_PID"

# Wait for Flask to be ready
sleep 2

# Start Node.js
NODE_ENV=development npx tsx server/index.ts
