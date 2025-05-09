#!/usr/bin/env python3

import os
import json
import sys
import argparse
from pathlib import Path
import shutil

# Create parser
parser = argparse.ArgumentParser(description="Setup and test Gnosara SocialBu system")
parser.add_argument("--clean", action="store_true", help="Clear existing data and start fresh")
parser.add_argument("--sample", action="store_true", help="Create sample data for testing")
parser.add_argument("--test", action="store_true", help="Run test in dry run mode")
parser.add_argument("--full", action="store_true", help="Perform clean, sample, and test")
args = parser.parse_args()

# Default to full flow if no arguments provided
if len(sys.argv) == 1:
    args.full = True

# Function to create required directories
def create_directories():
    print("📁 Creating required directories...")
    
    dirs = [
        "logs",
        "logs/reports",
        "logs/raw_responses",
        "logs/fixed_json",
        "summaries",
        "salvage"
    ]
    
    for directory in dirs:
        Path(directory).mkdir(exist_ok=True, parents=True)
        print(f"  ✅ Created: {directory}")

# Function to clean existing data
def clean_data():
    print("🧹 Cleaning existing data...")
    
    files_to_remove = [
        "processing_queue.json",
        "logs/summary_status.json",
        "logs/post_log.json",
        "logs/daily_log.json"
    ]
    
    for file_path in files_to_remove:
        if Path(file_path).exists():
            Path(file_path).unlink()
            print(f"  ✅ Removed: {file_path}")
    
    # Clean directories but keep the structure
    dirs_to_clean = [
        "summaries",
        "logs/reports",
        "logs/raw_responses",
        "logs/fixed_json",
        "salvage"
    ]
    
    for dir_path in dirs_to_clean:
        if Path(dir_path).exists():
            for file in Path(dir_path).glob("*"):
                if file.is_file():
                    file.unlink()
            print(f"  ✅ Cleaned: {dir_path}")

# Function to create sample data
def create_sample_data():
    print("🧪 Creating sample data...")
    
    # Create sample processing queue with string IDs (to test the fix)
    sample_queue = [
        "dQw4w9WgXcQ",
        "xvFZjo5PgG0"
    ]
    
    # Save sample queue
    queue_path = Path("processing_queue.json")
    queue_path.write_text(json.dumps(sample_queue, indent=2))
    print(f"  ✅ Created: {queue_path}")
    
    # Create sample summary status
    status = {
        "date": "2025-04-23",
        "pending": [],
        "batched": [],
        "completed": [],
        "failed": [],
        "posted": []
    }
    
    # Save sample status
    status_path = Path("logs/summary_status.json")
    status_path.write_text(json.dumps(status, indent=2))
    print(f"  ✅ Created: {status_path}")
    
    # Create sample daily log
    daily_log = {
        "date": "2025-04-23",
        "summarized": [],
        "posted": {},
        "errors": [],
        "pending": []
    }
    
    # Save sample daily log
    daily_log_path = Path("logs/daily_log.json")
    daily_log_path.write_text(json.dumps(daily_log, indent=2))
    print(f"  ✅ Created: {daily_log_path}")
    
    # Create a sample summary
    sample_summary = {
        "title": "The Future of AI: Opportunities and Challenges",
        "podcaster": "Tech Insights Podcast",
        "guest": "Dr. Jane Smith",
        "video_id": "dQw4w9WgXcQ",
        "video_url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "summary": {
            "essence": "Dr. Jane Smith explains how AI is transforming industries while creating new ethical challenges. She believes we're at a critical juncture where the decisions we make today will shape AI's impact for decades to come.",
            
            "top_takeaways": [
                "AI is advancing faster than regulatory frameworks can adapt",
                "The biggest near-term impact will be in healthcare and transportation",
                "Small businesses can leverage AI tools to compete with larger corporations",
                "Job displacement concerns are valid but new roles are emerging simultaneously",
                "The real question isn't whether AI will change everything, but whether we'll be thoughtful about how we implement it."
            ],
            
            "game_changing_ideas": [
                "AI systems should be designed with human augmentation in mind, not replacement",
                "We need 'AI translators' who can bridge technical capabilities with business needs",
                "Ethical AI development requires diverse teams from various backgrounds",
                "Open source AI models may ultimately be safer than proprietary ones",
                "I believe we're moving toward a world where AI literacy will be as fundamental as reading and writing."
            ],
            
            "things_you_can_do": [
                "Start experimenting with AI tools in your workflow, even in small ways",
                "Join online communities discussing ethical AI development",
                "Invest time in learning prompt engineering for better AI interactions",
                "Consider how your business data could be leveraged by AI systems"
            ],
            
            "why_this_matters": "AI represents the most significant technological shift since the internet. It will transform how we work, create, and solve problems. Understanding its capabilities and limitations now will position you to thrive in an AI-augmented future, while helping ensure these powerful tools are developed responsibly. The companies and individuals who thoughtfully integrate AI today will have a significant advantage in the coming years."
        }
    }
    
    # Save sample summary
    summary_path = Path("summaries/dQw4w9WgXcQ_sample_ai_podcast.json")
    summary_path.write_text(json.dumps(sample_summary, indent=2))
    print(f"  ✅ Created: {summary_path}")

# Function to run a test in dry run mode
def run_test():
    print("🧪 Running test in dry run mode...")
    print("=" * 80)
    
    cmd = "python3 post_scheduler.py --dry-run"
    exit_code = os.system(cmd)
    
    print("=" * 80)
    if exit_code == 0:
        print("✅ Test completed successfully!")
    else:
        print("❌ Test failed with exit code:", exit_code // 256)

# Main execution
if __name__ == "__main__":
    create_directories()
    
    if args.clean or args.full:
        clean_data()
    
    if args.sample or args.full:
        create_sample_data()
    
    if args.test or args.full:
        run_test()
    
    print("🎉 Setup complete!")
