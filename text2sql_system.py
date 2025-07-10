"""
Text-to-SQL System with Multi-Agent Architecture
Main system class that orchestrates SQL generation, error reasoning, and error fixing agents.
"""

import dspy
import pandas as pd
import re
from typing import Dict, List, Any
from agents import sql_program, error_reasoning_program, error_fix_agent


def clean_llm_response(text: str) -> str:
    """
    Helper function to clean LLM response and extract SQL code
    
    Args:
        text: Raw LLM response text
        
    Returns:
        Cleaned SQL query string
    """
    # Remove markdown code blocks
    splits = text.split('```')
    if len(splits) >= 3:
        # Extract content from code blocks
        sql_content = splits[1].replace('sql', '').strip()
    else:
        sql_content = text.strip()
    
    # Clean up common LLM artifacts
    sql_content = sql_content.replace('sql', '', 1)  # Remove leading 'sql'
    
    # Remove common completion artifacts
    completion_markers = ['### Completed:', '### End', '# Completed', 'Completed:', '###', '#']
    for marker in completion_markers:
        if marker in sql_content:
            sql_content = sql_content.split(marker)[0].strip()
    
    # Remove newlines and extra spaces
    sql_content = ' '.join(sql_content.split())
    
    return sql_content


class Text2SQLSystem(dspy.Module):
    """
    Main Text-to-SQL system using multi-agent architecture with DSPy and Ollama
    """
    
    def __init__(self, 
                 sql_model: str = "gpt-4o-mini",
                 error_reasoning_model: str = "gpt-4o", 
                 error_fix_model: str = "gpt-4o-mini",
                 max_retry: int = 3, 
                 api_key: str = None):
        """
        Initialize the Text2SQL system with different OpenAI models for each agent
        
        Args:
            sql_model: OpenAI model for SQL generation agent (e.g., 'gpt-4o-mini', 'gpt-4o')
            error_reasoning_model: OpenAI model for error reasoning agent  
            error_fix_model: OpenAI model for error fixing agent
            max_retry: Maximum number of retry attempts for failed queries
            api_key: OpenAI API key (if None, will use OPENAI_API_KEY environment variable)
        """
        super().__init__()
        self.max_retry = max_retry
        
        # Handle API key
        import os
        if api_key is None:
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise ValueError(
                    "OpenAI API key is required. Either:\n"
                    "1. Pass api_key parameter, or\n"
                    "2. Set OPENAI_API_KEY environment variable"
                )
        
        # Configure individual OpenAI models for each agent
        try:
            # SQL Agent Model
            self.sql_lm = dspy.LM(
                model=f"openai/{sql_model}", 
                api_key=api_key,
                temperature=0.0, 
                max_tokens=2000
            )
            print(f"✅ SQL Agent connected to OpenAI: {sql_model}")
            
            # Error Reasoning Agent Model  
            self.reasoning_lm = dspy.LM(
                model=f"openai/{error_reasoning_model}", 
                api_key=api_key,
                temperature=0.0, 
                max_tokens=2000
            )
            print(f"✅ Error Reasoning Agent connected to OpenAI: {error_reasoning_model}")
            
            # Error Fix Agent Model
            self.fix_lm = dspy.LM(
                model=f"openai/{error_fix_model}", 
                api_key=api_key,
                temperature=0.0, 
                max_tokens=2000
            )
            print(f"✅ Error Fix Agent connected to OpenAI: {error_fix_model}")
            
        except Exception as e:
            print(f"❌ Failed to connect to OpenAI models: {e}")
            print("Please check your API key and internet connection")
            raise
            
        # Initialize the three agents with their specific models
        with dspy.context(lm=self.sql_lm):
            self.sql_agent = dspy.Predict(sql_program)
            
        with dspy.context(lm=self.reasoning_lm):
            self.error_reasoning_agent = dspy.Predict(error_reasoning_program)
            
        with dspy.context(lm=self.fix_lm):
            self.error_fix_agent = dspy.ChainOfThought(error_fix_agent)
        
        print("🤖 Text2SQL agents initialized successfully with OpenAI models")
    
    def forward(self, query: str, relevant_context: str, db_connector) -> Dict[str, List[Any]]:
        """
        Main forward method for the DSPy module
        
        Args:
            query: Natural language query from user
            relevant_context: Database schema and metadata information
            db_connector: Database connector object (DatabaseConnector or DuckDB connection)
            
        Returns:
            Dictionary containing responses, SQL queries, error reasons, and results
        """
        return self.query(query, relevant_context, db_connector)
    
    def query(self, query: str, database_info: str, db_connector) -> Dict[str, List[Any]]:
        """
        Process a natural language query and return SQL results
        
        Args:
            query: Natural language query from user
            database_info: Database schema and metadata information
            db_connector: Database connector object (DatabaseConnector or DuckDB engine)
            
        Returns:
            Dictionary containing:
            - 'response': List of agent responses
            - 'sql': List of generated SQL queries
            - 'error_reason': List of error explanations
            - 'df': List of result dataframes
        """
        df = pd.DataFrame()
        return_dict = {
            'response': [],
            'sql': [],
            'error_reason': [],
            'df': []
        }
        
        print(f"\n🔍 Processing query: {query}")
        
        # Initial SQL generation
        try:
            with dspy.context(lm=self.sql_lm):
                response = self.sql_agent(user_query=query, dataset_information=database_info)
            return_dict['response'].append(response)
            print("✅ Initial SQL generated")
        except Exception as e:
            print(f"❌ Failed to generate initial SQL: {e}")
            return return_dict
        
        information = {'user_query': query, 'relevant_context': database_info}
        retry_count = 0
        
        # Retry loop for error correction
        while retry_count < self.max_retry:
            print(f"\n🔄 Attempt {retry_count + 1}/{self.max_retry}")
            
            # Clean and extract SQL from response
            sql = clean_llm_response(response.generated_sql)
            print(f"Generated SQL: {sql}")
            
            try:
                return_dict['sql'].append(sql)
                
                # Execute SQL query using database connector
                if hasattr(db_connector, 'execute_query'):
                    # Using DatabaseConnector
                    df = db_connector.execute_query(sql)
                else:
                    # Using DuckDB engine (backwards compatibility)
                    df = db_connector.execute(sql).df()
                
                return_dict['df'].append(df)
                
                if df.empty:
                    raise Exception("Query returned empty result set")
                else:
                    print("✅ Query executed successfully!")
                    print(f"📊 Result shape: {df.shape}")
                    retry_count = self.max_retry + 1  # Exit loop
                    
            except Exception as e:
                error_msg = str(e)
                print(f"❌ Query failed: {error_msg[:100]}...")
                
                try:
                    # Generate error reasoning
                    with dspy.context(lm=self.reasoning_lm):
                        error_reason = self.error_reasoning_agent(
                            error_message=error_msg[:500],  # Limit error message length
                            incorrect_sql=sql,
                            information=str(information)
                        )
                    
                    # Check if user asked a SQL-related question
                    if 'NOT ASKING FOR SQL' not in error_reason.error_fix_reasoning:
                        return_dict['error_reason'].append(error_reason.error_fix_reasoning)
                        print("🔧 Error analysis completed")
                        
                        # Generate corrected SQL
                        with dspy.context(lm=self.fix_lm):
                            response = self.error_fix_agent(instruction=error_reason.error_fix_reasoning)
                        return_dict['response'].append(response)
                        print("🔄 Correction attempt generated")
                    else:
                        print("❌ Query not SQL-related")
                        retry_count = self.max_retry + 1  # Exit loop
                        
                except Exception as reasoning_error:
                    print(f"❌ Error in reasoning/fixing: {reasoning_error}")
                    retry_count = self.max_retry + 1  # Exit loop
            
            retry_count += 1
        
        return return_dict
    
    def get_last_successful_result(self, result_dict: Dict[str, List[Any]]) -> tuple:
        """
        Extract the last successful SQL query and result from the return dictionary
        
        Args:
            result_dict: Result dictionary from query method
            
        Returns:
            Tuple of (sql_query, dataframe) or (None, None) if no successful result
        """
        if result_dict['sql'] and result_dict['df']:
            return result_dict['sql'][-1], result_dict['df'][-1]
        return None, None
    
    def display_results(self, result_dict: Dict[str, List[Any]], max_rows: int = 10) -> None:
        """
        Display the results in a formatted way
        
        Args:
            result_dict: Result dictionary from query method
            max_rows: Maximum number of rows to display
        """
        sql, df = self.get_last_successful_result(result_dict)
        
        if sql and df is not None:
            print("\n" + "="*50)
            print("🎯 FINAL RESULTS")
            print("="*50)
            print(f"SQL Query:\n{sql}")
            print(f"\nResults ({len(df)} rows):")
            print(df.head(max_rows).to_string(index=False))
            if len(df) > max_rows:
                print(f"... and {len(df) - max_rows} more rows")
        else:
            print("\n❌ No successful results to display")
            if result_dict['error_reason']:
                print(f"Last error reasoning: {result_dict['error_reason'][-1]}")


# Convenience function for quick usage
def create_text2sql_system(
    sql_model: str = "gpt-4o-mini",
    error_reasoning_model: str = "gpt-4o",
    error_fix_model: str = "gpt-4o-mini", 
    max_retry: int = 3,
    api_key: str = None
) -> Text2SQLSystem:
    """
    Create and return a Text2SQL system instance with specific OpenAI models for each agent
    
    Args:
        sql_model: OpenAI model for SQL generation agent
        error_reasoning_model: OpenAI model for error reasoning agent
        error_fix_model: OpenAI model for error fixing agent
        max_retry: Maximum retry attempts
        api_key: OpenAI API key (if None, will use OPENAI_API_KEY environment variable)
        
    Returns:
        Configured Text2SQLSystem instance
    """
    return Text2SQLSystem(
        sql_model=sql_model,
        error_reasoning_model=error_reasoning_model, 
        error_fix_model=error_fix_model,
        max_retry=max_retry,
        api_key=api_key
    ) 