# Multi-Agent Text-to-SQL System

A sophisticated text-to-SQL system that uses multiple AI agents to generate, validate, and fix SQL queries based on natural language input. The system employs a multi-agent approach with error reasoning and self-correction capabilities.

## System Architecture

The system consists of three main agents:

1. **SQL Agent**: Generates initial SQL queries from natural language
2. **Error Reasoning Agent**: Analyzes failed queries and provides fix instructions  
3. **Error Fix Agent**: Applies corrections based on reasoning agent's instructions

## Features

- 🤖 Multi-agent architecture for robust SQL generation
- 🔄 Self-correcting queries with error reasoning
- 🌐 **Web UI with database connections** (MySQL, PostgreSQL, SQL Server)
- 💬 **Interactive chat interface** for natural language queries
- 📊 DuckDB integration for testing
- 🎯 OpenAI integration for powerful cloud-based LLM execution
- 📈 Comprehensive error handling and retry logic
- 📝 Detailed logging and debugging information
- 🔌 **Dynamic database connection management**
- 📋 **Automatic schema introspection** and context generation

## Prerequisites

- Python 3.8+
- OpenAI API key
- Internet connection for OpenAI API calls

## Installation

1. Clone the repository:
```bash
git clone <your-repo-url>
cd multi-agent-t2s
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up your OpenAI API key:
```bash
export OPENAI_API_KEY="your-api-key-here"
# Or create a .env file with: OPENAI_API_KEY=your-api-key-here
```

## Quick Start

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Set up OpenAI API:**
  ```bash
  # Get API key from https://platform.openai.com/api-keys
  export OPENAI_API_KEY=""
  ```

3. **Run the setup script:**
```bash
python setup.py
```

4. **Test the system:**
```bash
python test_system.py
```

5. **Launch Web UI:**
```bash
python run_web_ui.py
# Opens http://localhost:8501 in your browser
```

6. **Try command-line examples:**
```bash
python example_usage.py basic
# or
python example_usage.py interactive
```

7. **Use in your code:**
```python
from text2sql_system import Text2SQLSystem
from database_setup import create_sample_sales_database, get_database_schema_info

# Initialize system with different OpenAI models for each agent
system = Text2SQLSystem(
    sql_model="gpt-4o-mini",           # SQL generation agent
    error_reasoning_model="gpt-4o", # Error analysis agent  
    error_fix_model="gpt-4o-mini"      # Error correction agent
)

# Create sample database
conn = create_sample_sales_database()
schema_info = get_database_schema_info()

# Query the system
result = system.query(
    "What is the total sales volume by region?",
    schema_info,
    conn
)

# Display results
system.display_results(result)
```

## Database Schema

The system expects database information including:
- Table names and structures
- Column details and data types
- Sample data and statistics
- Common query patterns

## Configuration

Set up your environment variables in `.env`:
```
OPENAI_API_KEY=your-api-key-here
MAX_RETRY_ATTEMPTS=3
```

## Usage Examples

See `examples/` directory for detailed usage examples and sample databases.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details # text-to-sql
