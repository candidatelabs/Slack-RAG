import re

class CandidateExtractor:
    def __init__(self):
        self.linkedin_pattern = re.compile(r'https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9-]+/?\S*')

    def extract_candidates(self, message):
        candidates = []
        if message.get('type') == 'message' and 'text' in message:
            text = message['text']
            matches = self.linkedin_pattern.finditer(text)
            for match in matches:
                candidates.append(match.group(0))
        return candidates

    def extract_candidates_from_messages(self, messages):
        candidates = []
        for message in messages:
            candidates.extend(self.extract_candidates(message))
        print(f"Extracted candidates: {candidates}")  # Debug print
        return candidates 