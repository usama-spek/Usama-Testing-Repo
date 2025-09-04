import json
import pandas as pd
import ollama
from prefect import flow, task


@task
def load_transcript(file_path: str) -> str:
    with open(file_path, "r") as f:
        return f.read()


@task
def analyze_transcript(transcript: str) -> str:
    prompt = f"""
    You are an assistant that extracts structured information from meeting transcripts.

    Transcript:
    {transcript}

    Return ONLY valid JSON in this exact format, no explanation, no text outside JSON:

    {{
      "participants": [],
      "decisions": [],
      "action_items": [
        {{"owner": "", "task": "", "deadline": ""}}
      ]
    }}
    """

    response = ollama.chat(
        model="mistral",   # or llama3, gemma, phi3, etc.
        messages=[{"role": "user", "content": prompt}]
    )
    return response["message"]["content"]


@task
def parse_json(raw_output: str) -> dict:
    try:
        return json.loads(raw_output)
    except json.JSONDecodeError:
        raise ValueError("Model did not return valid JSON")


@task
def save_to_csv(data: dict, actions_file: str, decisions_file: str):
    df_actions = pd.DataFrame(data.get("action_items", []))
    df_decisions = pd.DataFrame(data.get("decisions", []), columns=["Decision"])

    df_actions.to_csv(actions_file, index=False)
    df_decisions.to_csv(decisions_file, index=False)

    return f"Saved {actions_file} and {decisions_file}"


@flow(name="Meeting Transcript Analyzer")
def transcript_pipeline(file_path: str):
    transcript = load_transcript(file_path)
    raw_output = analyze_transcript(transcript)
    data = parse_json(raw_output)
    result = save_to_csv(data, "meeting_actions.csv", "meeting_decisions.csv")
    print(result)


if __name__ == "__main__":
    transcript_pipeline("meeting_transcript.txt")
