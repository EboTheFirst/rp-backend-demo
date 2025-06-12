from typing_extensions import TypedDict, Annotated
from langchain.prompts import ChatPromptTemplate
from app.core.config import model

# === Output Schema ===
class ExtractedFilterObject(TypedDict):
    filter_object: Annotated[
        dict | None,
        """
        A JSON object representing the filter criteria using:
        - Logical operators: "and", "or", "not"
        - Leaf comparison expressions: { "column": "<column_name>", "operator": "<operator>", "value": "<value>" }
        The filter object can be infinitely nested and contain any number of conditions in logical operators.
        Return null if no filtering criteria are found.
        """
    ]

# === System Prompt ===
filter_extraction_system = """
You are a data filter extraction assistant. Your goal is to analyze a user's natural language query and convert it into a structured JSON filter object.

**Data Schema:**
{schema_prompt}

**Filter Object Structure:**
- Logical operators: "and", "or", "not"
- Logical operators can contain **any number of sub-filters or conditions** (not limited to two).
- Leaf comparison expressions: {{ "column": "<column_name>", "operator": "<operator>", "value": "<value>" }}
- The filter object can be **infinitely nested** to represent any level of logical complexity implied by the query.
- Return a valid JSON object representing the filter criteria using ONLY these columns.
- If no filtering criteria are present in the query, return null.

**Supported operators:**
- equals
- not_equals
- greater_than
- greater_than_equals
- less_than
- less_than_equals
- between
- in
- not_in

**Example Filter Objects:**

Example 1:
{{
  "filter_object": {{
    "and": [
      {{ "column": "amount", "operator": "greater_than", "value": 100 }},
      {{ "or": [
          {{ "column": "date", "operator": "between", "value": ["2021-01-01", "2021-01-31"] }},
          {{ "column": "channel", "operator": "equals", "value": "Online" }},
          {{ "column": "channel", "operator": "equals", "value": "Mobile" }}
      ]
      }}
    ]
  }}
}}

Example 2:
{{
  "filter_object": {{
    "not": {{
      "and": [
      {{ "column": "merchant_id", "operator": "equals", "value": "M-001" }},
      {{ "column": "amount", "operator": "less_than", "value": 50 }},
      {{ "column": "channel", "operator": "equals", "value": "POS" }}
      ]
    }}
  }}
}}

Example 3:
{{
  "filter_object": {{
    "or": [
      {{ "column": "customer_id", "operator": "equals", "value": "CUST-0001" }},
      {{ "column": "customer_id", "operator": "equals", "value": "CUST-0002" }},
    ]
  }}
}}
"""

# === User Prompt ===
filter_extraction_user = """
User query:
{query}

Generate the structured filter object.
"""

# === Build the Prompt Template ===
filter_extraction_prompt = ChatPromptTemplate.from_messages([
    ("system", filter_extraction_system),
    ("user", filter_extraction_user)
])

# === Final Chain ===
filter_extraction_chain = filter_extraction_prompt | model.with_structured_output(ExtractedFilterObject)
