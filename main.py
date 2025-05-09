from flask import Flask, jsonify
import os
import logging
from datetime import datetime
from pathlib import Path

# Set up logging
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/render_app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("render_app")

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({
        "status": "online",
        "message": "Podcast summary system is running",
        "endpoints": [
            "/check-playlists",
            "/process-queue",
            "/post-summaries",
            "/full-cycle"
        ]
    })

@app.route('/check-playlists', methods=['GET', 'POST'])
def check_playlists():
    try:
        logger.info("Running playlist check")
        from populate_queue_from_playlists import PlaylistMonitor
        monitor = PlaylistMonitor()
        result = monitor.run()
        status = "success" if result == 0 else "error"
        logger.info(f"Playlist check completed with status: {status}")
        return jsonify({"status": status})
    except Exception as e:
        logger.error(f"Error in check_playlists: {e}")
        return jsonify({"status": "error", "message": str(e)})

@app.route('/process-queue', methods=['GET', 'POST'])
def process_queue():
    try:
        logger.info("Processing queue")
        from post_scheduler import PostScheduler
        scheduler = PostScheduler()
        result = scheduler.check_and_process_queue()
        status = "success" if result else "no_action"
        logger.info(f"Queue processing completed with status: {status}")
        return jsonify({"status": status})
    except Exception as e:
        logger.error(f"Error in process_queue: {e}")
        return jsonify({"status": "error", "message": str(e)})

@app.route('/post-summaries', methods=['GET', 'POST'])
def post_summaries():
    try:
        logger.info("Posting summaries")
        from post_scheduler import PostScheduler
        scheduler = PostScheduler()
        result = scheduler.post_unposted_summaries()
        logger.info(f"Posted summaries: {result}")
        return jsonify({"posted": result})
    except Exception as e:
        logger.error(f"Error in post_summaries: {e}")
        return jsonify({"status": "error", "message": str(e)})

@app.route('/full-cycle', methods=['GET', 'POST'])
def full_cycle():
    try:
        logger.info("Running full cycle")
        from post_scheduler import PostScheduler
        scheduler = PostScheduler()
        result = scheduler.run_full_cycle()
        logger.info("Full cycle completed successfully")
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in full_cycle: {e}")
        return jsonify({"status": "error", "message": str(e)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)