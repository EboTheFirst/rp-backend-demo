from typing_extensions import TypedDict, Annotated
from langchain.prompts import ChatPromptTemplate
from app.core.config import model

class FilterIntentDecision(TypedDict):
    filter_intent: Annotated[
        bool,
        "True if the query is about filtering data, false otherwise."
    ]

intent_classification_system = """
You are a classification assistant. Your task is to analyze a user's natural language query and determine if it is a data filter request.

Return a JSON object: {{ "filter_intent": true }} or {{ "filter_intent": false }}
"""

intent_classification_user = """
User query:
{query}

Is this a data filter request?
"""

intent_classification_prompt = ChatPromptTemplate.from_messages([
    ("system", intent_classification_system),
    ("user", intent_classification_user)
])

intent_classification_chain = intent_classification_prompt | model.with_structured_output(FilterIntentDecision)
