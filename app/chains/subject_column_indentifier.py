from typing_extensions import TypedDict, Annotated
from langchain.prompts import ChatPromptTemplate
from app.core.config import model

# === Output Schema ===
class GroupByColumnExtraction(TypedDict):
    group_by_column: Annotated[
        str | None,
        "The column to group by for computing aggregated metrics. Return null if no grouping column is detected."
    ]

# === System Prompt ===
group_by_extraction_system = """
You are an assistant that identifies the most relevant column to group by when computing aggregated or filtered metrics from a natural language query.

Your task:
- Given the user's natural language query and the available entity ID columns, identify which column should be used to group the data.

Available entity ID columns:
{available_entity_id_columns}

Return a JSON object: {{ "group_by_column": "<entity_id_column>" }} if a grouping column is found, otherwise return null.
"""

# === User Prompt ===
group_by_extraction_user = """
User query:
{query}

What is the column to group by for this query?
"""

# === Build the Prompt Template ===
group_by_extraction_prompt = ChatPromptTemplate.from_messages([
    ("system", group_by_extraction_system),
    ("user", group_by_extraction_user)
])

# === Final Chain ===
group_by_extraction_chain = group_by_extraction_prompt | model.with_structured_output(GroupByColumnExtraction)
