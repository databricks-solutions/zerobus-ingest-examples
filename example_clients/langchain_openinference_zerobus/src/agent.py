"""A LangChain agent with one tool and a scripted chat model.

The scripted model makes no network calls, so the example runs without an LLM key.
It asks for the get_weather tool once, then answers with the tool's result.
Replace ScriptedChatModel with a real chat model (for example ChatOpenAI) to use a live model.
"""

import re
from typing import Any

from langchain.agents import create_agent
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import tool


@tool
def get_weather(city: str) -> str:
    """Get the current weather for a city."""
    return f"The weather in {city} is sunny and 21 C."


class ScriptedChatModel(BaseChatModel):
    """Offline chat model: one tool call, then a final answer."""

    @property
    def _llm_type(self) -> str:
        return "scripted-chat-model"

    def bind_tools(self, tools: Any, **kwargs: Any) -> "ScriptedChatModel":
        return self

    def _generate(self, messages: list[BaseMessage], stop: list[str] | None = None,
                  run_manager: Any = None, **kwargs: Any) -> ChatResult:
        last = messages[-1]
        if isinstance(last, ToolMessage):
            message = AIMessage(content=f"Answer: {last.content}")
        else:
            match = re.search(r"in (\w+)", str(last.content))
            city = match.group(1) if match else "Paris"
            message = AIMessage(
                content="",
                tool_calls=[{"name": "get_weather", "args": {"city": city}, "id": "call_1"}],
            )
        return ChatResult(generations=[ChatGeneration(message=message)])


def build_agent():
    return create_agent(ScriptedChatModel(), tools=[get_weather], name="weather_agent")


def ask(agent, question: str) -> str:
    result = agent.invoke({"messages": [{"role": "user", "content": question}]})
    return str(result["messages"][-1].content)
