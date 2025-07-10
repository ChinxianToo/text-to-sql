#!/usr/bin/env python3
"""
Launcher script for the Text-to-SQL Web UI
"""

import subprocess
import sys
import os
from pathlib import Path

def check_dependencies():
    """Check if required dependencies are installed"""
    try:
        import streamlit
        import sqlalchemy
        import openai
        from dotenv import load_dotenv
        print("✅ Required dependencies found")
        return True
    except ImportError as e:
        print(f"❌ Missing dependencies: {e}")
        print("Please run: pip install -r requirements.txt")
        return False

def check_and_setup_openai():
    """Check OpenAI configuration from environment variables and .env file"""
    try:
        import openai
        from dotenv import load_dotenv
        
        # Load environment variables from .env file
        load_dotenv()
        
        # Check if API key is available
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            print("❌ OpenAI API key not found!")
            return False
        
        # Test the API key
        print("🔍 Checking OpenAI API connection...")
        try:
            client = openai.OpenAI(api_key=api_key)
            models = client.models.list()
            print(f"✅ OpenAI API is ready! Found {len(models.data)} available models")
            return True
        except Exception as e:
            print(f"❌ OpenAI API test failed: {str(e)}")
            print("Please check your API key in the .env file or environment variables")
            return False
                
    except ImportError:
        print("❌ Required packages not installed. Please run: pip install -r requirements.txt")
        return False

def main():
    """Launch the Streamlit web UI"""
    print("🚀 Starting Text-to-SQL Web UI")
    print("="*50)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Check and setup OpenAI
    if not check_and_setup_openai():
        print("\n❌ OpenAI setup failed. Please configure your API key and try again.")
        sys.exit(1)
    
    # Get the directory of this script
    script_dir = Path(__file__).parent
    web_ui_path = script_dir / "web_ui.py"
    
    if not web_ui_path.exists():
        print(f"❌ Web UI file not found: {web_ui_path}")
        sys.exit(1)
    
    print("\n🌐 Launching web interface...")
    print("📱 The app will open in your browser automatically")
    print("🔗 URL: http://localhost:8501")
    print("\n⚠️  To stop the server, press Ctrl+C in this terminal")
    print("="*50)
    
    # Launch Streamlit
    try:
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", str(web_ui_path),
            "--server.address", "localhost",
            "--server.port", "8501",
            "--browser.gatherUsageStats", "false"
        ])
    except KeyboardInterrupt:
        print("\n\n🛑 Server stopped by user")
    except Exception as e:
        print(f"\n❌ Failed to start server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 