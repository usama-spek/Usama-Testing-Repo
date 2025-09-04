import ollama
import json
import pandas as pd

# Load transcript
with open("meeting_transcript.txt", "r") as f:
    transcript = f.read()

# Prompt
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

# Run with Ollama (local LLM)
response = ollama.chat(
    model="mistral",   # or llama3, gemma, phi3, etc.
    messages=[{"role": "user", "content": prompt}]
)

# The model's reply
raw_output = response["message"]["content"]
print("Raw model output:\n", raw_output)

# Parse JSON
try:
    data = json.loads(raw_output)
except json.JSONDecodeError:
    print("⚠️ The model did not return valid JSON.")
    exit()

# Save to CSV
df_actions = pd.DataFrame(data.get("action_items", []))
df_decisions = pd.DataFrame(data.get("decisions", []), columns=["Decision"])

df_actions.to_csv("meeting_actions.csv", index=False)
df_decisions.to_csv("meeting_decisions.csv", index=False)

print("✅ Data saved to CSV")
