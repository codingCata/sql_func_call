from openai import OpenAI


def send_message_tool(messages, func):
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=messages,
        tools=func
    )
    return response.choices[0].message


client = OpenAI(
    api_key="your-api-key",
    base_url="https://api.deepseek.com",
)

