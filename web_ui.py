#!/usr/bin/env python3
"""
Web UI for Multi-Agent Text-to-SQL System
Built with Streamlit for interactive database connections and SQL generation.
"""

import streamlit as st
import pandas as pd
from typing import Dict, List, Any, Optional
import time
import traceback
from datetime import datetime

# Import our modules
from text2sql_system import Text2SQLSystem
from database_connectors import DatabaseConnector, test_database_connection
from database_setup import create_sample_sales_database, get_database_schema_info

# Page configuration
st.set_page_config(
    page_title="🤖 Text-to-SQL Agent",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    .connection-status {
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
        font-weight: bold;
    }
    
    .connected {
        background-color: #d4edda;
        color: #155724;
        border: 1px solid #c3e6cb;
    }
    
    .disconnected {
        background-color: #f8d7da;
        color: #721c24;
        border: 1px solid #f5c6cb;
    }
    
    .chat-message {
        padding: 1rem;
        margin: 0.5rem 0;
        border-radius: 0.5rem;
    }
    
    .user-message {
        background-color: #e3f2fd;
        border-left: 4px solid #2196f3;
    }
    
    .bot-message {
        background-color: #f3e5f5;
        border-left: 4px solid #9c27b0;
    }
    
    .error-message {
        background-color: #ffebee;
        border-left: 4px solid #f44336;
    }
    
    .sql-code {
        background-color: #f5f5f5;
        border: 1px solid #ddd;
        border-radius: 0.25rem;
        padding: 1rem;
        font-family: 'Courier New', monospace;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
def initialize_session_state():
    """Initialize Streamlit session state variables"""
    if 'db_connector' not in st.session_state:
        st.session_state.db_connector = None
    if 'text2sql_system' not in st.session_state:
        st.session_state.text2sql_system = None
    if 'is_connected' not in st.session_state:
        st.session_state.is_connected = False
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    if 'schema_info' not in st.session_state:
        st.session_state.schema_info = ""
    if 'connection_info' not in st.session_state:
        st.session_state.connection_info = {}

def show_connection_dialog():
    """Display database connection dialog"""
    st.sidebar.header("🔌 Database Connection")
    
    # Database type selection
    db_type = st.sidebar.selectbox(
        "Database Type",
        options=['mysql', 'postgresql', 'mssql'],
        format_func=lambda x: {
            'mysql': '🐬 MySQL',
            'postgresql': '🐘 PostgreSQL', 
            'mssql': '💼 SQL Server'
        }[x]
    )
    
    # Connection form
    with st.sidebar.form("db_connection_form"):
        st.subheader("Connection Details")
        
        # Get default port
        default_ports = {'mysql': 3306, 'postgresql': 5432, 'mssql': 1433}
        default_port = default_ports.get(db_type, 3306)
        
        title = st.text_input("Connection Title", value=f"My {db_type.title()} DB")
        host = st.text_input("Host", value="localhost")
        port = st.number_input("Port", value=default_port, min_value=1, max_value=65535)
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        database = st.text_input("Database (optional)")
        
        # SSL options
        ssl_options = {
            'mysql': ['preferred', 'disabled', 'required'],
            'postgresql': ['prefer', 'disable', 'require'],
            'mssql': ['yes', 'no']
        }
        ssl = st.selectbox("SSL", options=ssl_options.get(db_type, ['preferred']))
        
        # Form buttons
        col1, col2 = st.columns(2)
        test_clicked = col1.form_submit_button("🧪 Test", use_container_width=True)
        connect_clicked = col2.form_submit_button("🔗 Connect", use_container_width=True)
        
        # Handle form submissions
        if test_clicked:
            if not all([host, username, password]):
                st.error("Please fill in Host, Username, and Password")
            else:
                with st.spinner("Testing connection..."):
                    success, message = test_database_connection(db_type, host, port, username, password, database, ssl)
                    if success:
                        st.success(f"✅ {message}")
                    else:
                        st.error(f"❌ {message}")
        
        if connect_clicked:
            if not all([host, username, password]):
                st.error("Please fill in Host, Username, and Password")
            else:
                connect_to_database(db_type, host, port, username, password, database, ssl, title)

def connect_to_database(db_type: str, host: str, port: int, username: str, 
                       password: str, database: str, ssl: str, title: str):
    """Connect to database and initialize text2sql system"""
    with st.spinner("Connecting to database..."):
        try:
            # Create database connector
            connector = DatabaseConnector()
            success = connector.connect(db_type, host, port, username, password, database, ssl)
            
            if success:
                st.session_state.db_connector = connector
                st.session_state.is_connected = True
                st.session_state.connection_info = {
                    'title': title,
                    'db_type': db_type,
                    'host': host,
                    'port': port,
                    'database': database,
                    'connected_at': datetime.now()
                }
                
                # Generate schema info
                schema_info = connector.generate_schema_info(database)
                st.session_state.schema_info = schema_info
                
                # Initialize Text2SQL system if not already done
                if st.session_state.text2sql_system is None:
                    model_name = st.session_state.get('selected_model', 'gpt-4o-mini')
                    st.session_state.text2sql_system = Text2SQLSystem(
                        sql_model="gpt-4o-mini",
                        error_reasoning_model="gpt-4o", 
                        error_fix_model="gpt-4o-mini",
                        max_retry=3
                    )
                
                st.success(f"✅ Connected to {title}")
                st.rerun()
                
            else:
                st.error("❌ Failed to connect to database")
                
        except Exception as e:
            st.error(f"❌ Connection error: {str(e)}")

def show_connection_status():
    """Display current connection status"""
    if st.session_state.is_connected and st.session_state.db_connector:
        info = st.session_state.connection_info
        status_html = f"""
        <div class="connection-status connected">
            🟢 Connected to <strong>{info['title']}</strong><br>
            📍 {info['db_type'].title()} • {info['host']}:{info['port']}<br>
            🕐 Connected at {info['connected_at'].strftime('%H:%M:%S')}
        </div>
        """
        st.sidebar.markdown(status_html, unsafe_allow_html=True)
        
        # Disconnect button
        if st.sidebar.button("🔌 Disconnect", use_container_width=True):
            disconnect_from_database()
    else:
        status_html = """
        <div class="connection-status disconnected">
            🔴 Not connected to any database
        </div>
        """
        st.sidebar.markdown(status_html, unsafe_allow_html=True)

def disconnect_from_database():
    """Disconnect from current database"""
    if st.session_state.db_connector:
        st.session_state.db_connector.disconnect()
    
    st.session_state.db_connector = None
    st.session_state.is_connected = False
    st.session_state.connection_info = {}
    st.session_state.schema_info = ""
    st.session_state.chat_history = []
    
    st.success("🔌 Disconnected from database")
    st.rerun()

def show_sample_database_option():
    """Show option to use sample database"""
    st.sidebar.markdown("---")
    st.sidebar.subheader("🧪 Try Sample Database")
    st.sidebar.markdown("Want to test the system? Use our built-in sample database with timber sales data.")
    
    if st.sidebar.button("🚀 Use Sample Database", use_container_width=True):
        use_sample_database()

def use_sample_database():
    """Connect to the sample DuckDB database"""
    with st.spinner("Setting up sample database..."):
        try:
            # Create sample database
            from database_setup import create_sample_sales_database, get_database_schema_info
            db_conn = create_sample_sales_database()
            
            # Store in session state (different from DatabaseConnector)
            st.session_state.db_connector = db_conn  # DuckDB connection
            st.session_state.is_connected = True
            st.session_state.connection_info = {
                'title': 'Sample Timber Sales DB',
                'db_type': 'duckdb',
                'host': 'localhost',
                'port': 'memory',
                'database': 'sample',
                'connected_at': datetime.now()
            }
            
            # Get schema info
            schema_info = get_database_schema_info()
            st.session_state.schema_info = schema_info
            
            # Initialize Text2SQL system
            if st.session_state.text2sql_system is None:
                model_name = st.session_state.get('selected_model', 'gpt-4o-mini')
                st.session_state.text2sql_system = Text2SQLSystem(
                    sql_model="gpt-4o-mini",
                    error_reasoning_model="gpt-4o", 
                    error_fix_model="gpt-4o-mini",
                    max_retry=3
                )
            
            st.success("✅ Sample database ready!")
            st.rerun()
            
        except Exception as e:
            st.error(f"❌ Failed to setup sample database: {str(e)}")

def show_model_selection():
    """Show model selection in sidebar"""
    st.sidebar.markdown("---")
    st.sidebar.subheader("🤖 AI Model Selection")
    
    # Common OpenAI models
    model_options = [
        'gpt-4o-mini',
        'gpt-4o',
        'gpt-4-turbo',
        'gpt-3.5-turbo',
        'custom'
    ]
    
    selected = st.sidebar.selectbox(
        "Choose OpenAI Model",
        options=model_options,
        index=0 if 'selected_model' not in st.session_state else model_options.index(st.session_state.get('selected_model', model_options[0]))
    )
    
    if selected == 'custom':
        custom_model = st.sidebar.text_input("Custom Model Name", value="gpt-4o-mini")
        st.session_state.selected_model = custom_model
    else:
        st.session_state.selected_model = selected
    
    # Reinitialize system if model changed
    if st.session_state.text2sql_system and hasattr(st.session_state.text2sql_system, 'model_name'):
        current_model = getattr(st.session_state.text2sql_system, 'model_name', '')
        if current_model != st.session_state.selected_model:
            st.session_state.text2sql_system = None

def show_chat_interface():
    """Display the main chat interface"""
    st.markdown('<div class="main-header">🤖 Text-to-SQL Agent</div>', unsafe_allow_html=True)
    
    if not st.session_state.is_connected:
        st.markdown("""
        ### Welcome to the Text-to-SQL Agent! 👋
        
        To get started:
        1. **Connect to your database** using the sidebar (MySQL, PostgreSQL, or SQL Server)
        2. **Or try our sample database** for a quick demo
        3. **Start asking questions** in natural language!
        
        Example questions you can ask:
        - *"Show me the top 10 customers by revenue"*
        - *"What were our sales last month?"*
        - *"Which products are selling best?"*
        """)
        return
    
    # Chat input
    query = st.chat_input("Ask a question about your data...")
    
    if query:
        process_user_query(query)
    
    # Display chat history
    for message in st.session_state.chat_history:
        display_chat_message(message)

def process_user_query(query: str):
    """Process user query and generate SQL response"""
    # Add user message to chat
    st.session_state.chat_history.append({
        'type': 'user',
        'content': query,
        'timestamp': datetime.now()
    })
    
    if not st.session_state.text2sql_system:
        # Try to initialize system
        try:
            model_name = st.session_state.get('selected_model', 'gpt-4o-mini')
            st.session_state.text2sql_system = Text2SQLSystem(
                sql_model="gpt-4o-mini",
                error_reasoning_model="gpt-4o", 
                error_fix_model="gpt-4o-mini",
                max_retry=3
            )
        except Exception as e:
            st.session_state.chat_history.append({
                'type': 'error',
                'content': f"Failed to initialize AI system: {str(e)}",
                'timestamp': datetime.now()
            })
            return
    
    # Process query
    try:
        with st.spinner("🤖 Thinking..."):
            result = st.session_state.text2sql_system.query(
                query, 
                st.session_state.schema_info, 
                st.session_state.db_connector
            )
        
        # Get final result
        sql, df = st.session_state.text2sql_system.get_last_successful_result(result)
        
        if sql and df is not None:
            # Success response
            response_content = {
                'sql': sql,
                'dataframe': df,
                'error_corrections': result.get('error_reason', []),
                'attempts': len(result.get('sql', []))
            }
            
            st.session_state.chat_history.append({
                'type': 'bot_success',
                'content': response_content,
                'timestamp': datetime.now()
            })
        else:
            # Failed response
            error_msg = "I couldn't generate a working SQL query for your question."
            if result.get('error_reason'):
                error_msg += f" Last error: {result['error_reason'][-1][:200]}..."
            
            st.session_state.chat_history.append({
                'type': 'bot_error',
                'content': error_msg,
                'timestamp': datetime.now()
            })
            
    except Exception as e:
        st.session_state.chat_history.append({
            'type': 'error',
            'content': f"Error processing query: {str(e)}",
            'timestamp': datetime.now()
        })

def display_chat_message(message: dict):
    """Display a single chat message"""
    timestamp = message['timestamp'].strftime('%H:%M:%S')
    
    if message['type'] == 'user':
        with st.chat_message("user"):
            st.write(f"**You** • {timestamp}")
            st.write(message['content'])
    
    elif message['type'] == 'bot_success':
        with st.chat_message("assistant"):
            st.write(f"**AI Assistant** • {timestamp}")
            content = message['content']
            
            # Show SQL query
            st.write("Generated SQL:")
            st.code(content['sql'], language='sql')
            
            # Show results
            df = content['dataframe']
            st.write(f"Results ({len(df)} rows):")
            if len(df) > 0:
                st.dataframe(df, use_container_width=True)
            else:
                st.write("No results returned.")
            
            # Show error corrections if any
            if content['error_corrections']:
                with st.expander(f"🔧 Error Corrections ({len(content['error_corrections'])})"):
                    for i, correction in enumerate(content['error_corrections'], 1):
                        st.write(f"**Attempt {i}:** {correction[:200]}...")
            
            # Show attempt count
            if content['attempts'] > 1:
                st.caption(f"✅ Succeeded after {content['attempts']} attempts")
    
    elif message['type'] == 'bot_error':
        with st.chat_message("assistant"):
            st.write(f"**AI Assistant** • {timestamp}")
            st.error(message['content'])
    
    elif message['type'] == 'error':
        with st.chat_message("assistant"):
            st.write(f"**System** • {timestamp}")
            st.error(message['content'])

def show_database_info():
    """Show database information in sidebar"""
    if st.session_state.is_connected and st.session_state.schema_info:
        st.sidebar.markdown("---")
        st.sidebar.subheader("📊 Database Schema")
        
        with st.sidebar.expander("View Schema Details"):
            st.text(st.session_state.schema_info[:1000] + "..." if len(st.session_state.schema_info) > 1000 else st.session_state.schema_info)

def show_chat_controls():
    """Show chat control buttons"""
    if st.session_state.chat_history:
        st.sidebar.markdown("---")
        st.sidebar.subheader("💬 Chat Controls")
        
        if st.sidebar.button("🗑️ Clear Chat", use_container_width=True):
            st.session_state.chat_history = []
            st.rerun()

def main():
    """Main application function"""
    initialize_session_state()
    
    # Sidebar
    show_connection_status()
    if not st.session_state.is_connected:
        show_connection_dialog()
        show_sample_database_option()
    show_model_selection()
    show_database_info()
    show_chat_controls()
    
    # Main content
    show_chat_interface()
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; font-size: 0.8rem;'>
        🤖 Multi-Agent Text-to-SQL System • Built with Streamlit & DSPy • Powered by OpenAI
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main() 