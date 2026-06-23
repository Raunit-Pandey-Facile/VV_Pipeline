#!/bin/bash
echo "Starting VV Collation Pipeline..."
echo ""
echo "Once started, open your browser at:"
echo "  http://localhost:8502"
echo ""
echo "Press Ctrl+C to stop the app."
echo ""
streamlit run app.py \
    --server.headless true \
    --browser.gatherUsageStats false \
    --server.address localhost
