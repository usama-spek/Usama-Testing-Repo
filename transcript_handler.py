import json, os
import pandas as pd
import ollama
from prefect import flow, task, get_run_logger


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
    output_dir = "/Users/usamasheikh/Documents/prefect-outputs"
    os.makedirs(output_dir, exist_ok=True)

    df_actions.to_csv(f"{output_dir}/meeting_actions.csv", index=False)
    df_decisions.to_csv(f"{output_dir}/meeting_decisions.csv", index=False)
    # df_actions.to_csv(actions_file, index=False)
    # df_decisions.to_csv(decisions_file, index=False)

    return f"Saved {actions_file} and {decisions_file}"


@flow(name="Meeting Transcript Analyzer")
def transcript_pipeline(file_path: str):
    logger = get_run_logger()
    transcript = load_transcript(file_path)
    raw_output = analyze_transcript(transcript)
    data = parse_json(raw_output)
    result = save_to_csv(data, "meeting_actions.csv", "meeting_decisions.csv")
    logger.info(result)

OUTPUT_DIR = "/Users/usamasheikh/Documents/prefect-outputs"
PROCESSED_LOG = os.path.join(OUTPUT_DIR, "processed.log")


@flow(name="All Transcripts Analyzer")
def all_transcripts_flow(data_dir: str = "data"):
    logger = get_run_logger()
    # Load already processed files
    processed = set()
    if os.path.exists(PROCESSED_LOG):
        with open(PROCESSED_LOG) as f:
            processed = set(line.strip() for line in f)

    # Scan repo data folder
    files = [f for f in os.listdir(data_dir) if f.endswith(".txt")]

    for file in files:
        if file in processed:
            logger.info(f"⏭️ Skipping already processed file: {file}")
            continue

        # Run your existing flow as subflow
        transcript_pipeline(f"{data_dir}/{file}")

        # Append to processed log
        with open(PROCESSED_LOG, "a") as f:
            f.write(file + "\n")


if __name__ == "__main__":
    all_transcripts_flow("data")


