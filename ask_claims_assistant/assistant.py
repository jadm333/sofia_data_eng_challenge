from dotenv import load_dotenv
import os
from typing import Tuple, Optional
import pandas as pd
from io import StringIO
import re
from pathlib import Path
import json
import uuid
import subprocess
import tempfile

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase
from langchain_core.tools import tool
from utils.dbt_loader import get_schema_info_tool




def get_enhanced_prompt(question):
    return f"""
    You are a helpful Claim info assistant for a healthcare claims database. 
    
    Before answering any question, ALWAYS use the get_schema_info tool to understand the database structure.
    
    Answer the following question: {question}
    - First, get the schema information using the get_schema_info tool
    - Then, generate the correct SQL query based on the schema
    - Execute the query and return the results
    - Include the SQL query used for transparency
    - If the question involves trends, time series, charts, graphs, or visual analysis, use the generate_visualization tool

    The response must be a json in the following format:
    {{
        "natural_language_response": "Your answer in natural language",
        "sql_query": "The SQL query used to get the results",
        "visualization_path": "Path to the generated visualization file or None if not applicable",
        "error_info": "Error message if applicable"
    }}
    Make sure to format the SQL query correctly and ensure it is executable.
    """

def handle_question(question, agent):

    try:
        # Create enhanced prompt that instructs agent to use schema tool
        enhanced_prompt = get_enhanced_prompt(question)
        
        # Get response from agent
        response = agent.invoke({"input": enhanced_prompt})

        # Extract results
        result = response.get("output", "No results")
        
        return result
        
    except Exception as e:
        error_msg = f"Error processing question: {str(e)}"
        return error_msg
    

def create_visualization_tool(database, llm):
    """    Creates a tool for generating visualizations based on SQL query results.

    Args:
        database (SQLDatabase): The database connection to run queries.
        llm (ChatGoogleGenerativeAI): The language model to generate Python code for visualization.

    Returns:
        function: A tool function that generates visualizations.
    """
    
    # Define the tool function
    
    @tool
    def generate_visualization(sql_query: str, question: str) -> str:
        """        Generates a visualization based on the SQL query results and the question.

        Args:
            sql_query (str): The SQL query to execute.
            question (str): The question to answer with the visualization.

        Returns:
            str: Path to the generated visualization file or an error message.
        """
        try:
            # Execute query and get dataframe
            result = database.run(sql_query)
            df = pd.read_csv(StringIO(result), sep="\t")
            
            # Generate unique filename for the plot
            plot_filename = f"plot_{uuid.uuid4().hex[:8]}.png"

            
            
            # Get current working directory
            current_dir = Path(__file__).parent.resolve()

            plots_dir = Path(f"{current_dir}/tmp_plots")
            plots_dir.mkdir(parents=False, exist_ok=True)
            plot_path = plots_dir / plot_filename
            
            # Create prompt for the LLM to generate Python code
            code_generation_prompt = f"""
            Generate Python code to create a plotly visualization based on the following:
            
            Question: {question}
            DataFrame columns: {list(df.columns)}
            DataFrame shape: {df.shape}
            DataFrame sample: {df.head().to_string()}
            
            Requirements:
            1. Create a plotly figure that best answers the question
            2. The DataFrame is already loaded as 'df' and the plot path is loaded as 'plot_path'
            3. Save the figure as PNG using plotly's write_image method
            4. The plot should be saved in 'plot_path', the 'plot_path' variable is an pathlib.Path object
            5. Include proper titles and labels
            6. Handle different chart types based on the question (line for trends/time series, bar for comparisons, etc.)
            7. Only return the Python code, no explanations
            8. Import plotly.express as px and any other needed libraries
            9. Make sure the code is executable as-is
            
            Return only the Python code within triple backticks.
            """
            
            # Generate Python code using LLM
            code_response = llm.invoke(code_generation_prompt)
            
            # Extract code from response
            code_content = code_response.content
            if "```python" in code_content:
                code = code_content.split("```python")[1].split("```")[0].strip()
            elif "```" in code_content:
                code = code_content.split("```")[1].strip()
            else:
                code = code_content.strip()
            
            # Create a temporary Python file with the complete code
            temp_script_content = f"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from io import StringIO
plot_path = Path("{plot_path}")

# Load the dataframe
result = '''{result}'''
df = pd.read_csv(StringIO(result), sep="\\t")

# Generated visualization code
{code}
"""
            
            # Write to temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as temp_file:
                temp_file.write(temp_script_content)
                temp_script_path = temp_file.name
            
            try:
                # Execute the Python script in the conda environment
                cmd = [
                    "conda", "run", "-n", "sofia_data_eng_challenge", 
                    "python", temp_script_path
                ]
                
                result = subprocess.run(
                    cmd, 
                    capture_output=True, 
                    text=True,
                    cwd=str(current_dir)
                )
                
                if result.returncode == 0:
                    if plot_path.exists():
                        return f"Visualization saved to: {plot_path}"
                    else:
                        return f"Script executed successfully but plot file not found at {plot_path}"
                else:
                    return f"Error executing visualization script: {result.stderr}"
                    
            finally:
                # Clean up temporary file
                if os.path.exists(temp_script_path):
                    os.unlink(temp_script_path)
                
        except Exception as e:
            return f"Visualization error: {str(e)}"
    
    return generate_visualization

def ask_claims_mini_assistant(question = 'Show me the trends in claims over time by month'):
    # Get current path to locate the database
    parent_dir = Path(__file__).parent.parent.resolve()


    load_dotenv()

    llm = ChatGoogleGenerativeAI(model="gemini-2.5-pro", temperature=1.0)

    db = SQLDatabase.from_uri(f"duckdb:///{parent_dir}/data_modelling/data/db.duckdb")

    manifest_path = f"{parent_dir}/data_modelling/target/manifest.json"

    # Create enhanced SQL toolkit with schema information
    schema_tool = get_schema_info_tool(manifest_path)
    visualization_tool = create_visualization_tool(db, llm)
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)

    # Get the base tools from the toolkit and add custom tools
    base_tools = toolkit.get_tools()
    custom_tools = [schema_tool, visualization_tool]
    all_tools = base_tools + custom_tools
    
    # Create SQL agent with all tools
    agent = create_sql_agent(
        llm=llm,
        toolkit=toolkit,
        extra_tools=custom_tools,
        verbose=True,
        agent_type="openai-tools"  
    )

    result = handle_question(question=question, agent=agent)
    # Parse the result as JSON if it's wrapped in markdown code blocks
    try:
        # Remove markdown code block formatting if present
        if "```json" in result:
            json_content = result.split("```json")[1].split("```")[0].strip()
        elif "```" in result:
            json_content = result.split("```")[1].strip()
        else:
            json_content = result.strip()
        
        # Parse as JSON
        parsed_result = json.loads(json_content)
        
        return parsed_result
        
    except json.JSONDecodeError as e:
        return {"error": "Failed to parse JSON", "raw_result": result}
    except Exception as e:
        return {"error": "Unexpected error", "raw_result": result}

if __name__ == "__main__":
    ask_claims_mini_assistant()