from langchain_core.utils.function_calling import convert_to_openai_function


def convert_to_ds_function(cls):
    """Convert a class to a Deepseek function."""
    return {
        "type": "function",
        "function": convert_to_openai_function(cls)
    }


