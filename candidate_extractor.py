import re
from typing import List, Dict, Optional
import numpy as np

class CandidateExtractor:
    LINKEDIN_REGEX = re.compile(r'<(https?://(?:www\.)?linkedin\.com/in/[^>|]+)\|([^>]+)>')

    def __init__(self):
        self.candidates = []  # List of dicts: name, linkedin_url, message_id, timestamp, channel, etc.
        self.candidate_map = {}  # linkedin_url -> candidate dict
        self.associations = {}  # linkedin_url -> {'anchor':..., 'threads':[], 'direct':[], 'fuzzy':[]}

    def extract_candidates(self, messages: List[Dict], channel_name: str) -> List[Dict]:
        """
        Extract candidate anchors from Slack messages in a channel.
        Each anchor is a message with a LinkedIn URL and a candidate name.
        """
        for msg in messages:
            text = msg.get('text', '')
            for match in self.LINKEDIN_REGEX.finditer(text):
                linkedin_url = match.group(1)
                candidate_name = match.group(2)
                candidate = {
                    'name': candidate_name.strip(),
                    'linkedin_url': linkedin_url.strip(),
                    'message_id': msg.get('id') or msg.get('ts'),
                    'timestamp': msg.get('ts'),
                    'channel': channel_name,
                    'user': msg.get('user', ''),
                    'text': text
                }
                self.candidates.append(candidate)
                self.candidate_map[linkedin_url.strip()] = candidate
                self.associations[linkedin_url.strip()] = {
                    'anchor': candidate,
                    'threads': [],
                    'direct': [],
                    'fuzzy': []
                }
        return self.candidates

    def associate_threads(self, messages: List[Dict]):
        """
        For each candidate anchor, associate all thread replies.
        """
        # Build a map from thread_ts to candidate (using anchor message_id)
        thread_map = {c['anchor']['message_id']: url for url, c in self.associations.items()}
        for msg in messages:
            thread_ts = msg.get('thread_ts')
            if thread_ts and thread_ts in thread_map and msg.get('ts') != thread_ts:
                url = thread_map[thread_ts]
                self.associations[url]['threads'].append(msg)

    def associate_direct_mentions(self, messages: List[Dict]):
        """
        For all other channel messages, associate if they mention the candidate's name or LinkedIn URL.
        """
        for msg in messages:
            text = msg.get('text', '')
            for url, candidate in self.candidate_map.items():
                name = candidate['name']
                if url in text or name.lower() in text.lower():
                    # Avoid double-counting anchor or thread messages
                    if msg.get('id') == candidate['message_id']:
                        continue
                    if not msg.get('thread_ts') or msg.get('thread_ts') != candidate['message_id']:
                        self.associations[url]['direct'].append(msg)

    def associate_fuzzy(self, messages: List[Dict], rag_backend, channel_name):
        """
        For messages not matched above, use RAG backend's semantic_search to find likely candidate-related messages.
        """
        for msg in messages:
            text = msg.get('text', '')
            for url, candidate in self.candidate_map.items():
                # Skip if already associated
                if (msg in self.associations[url]['threads'] or
                    msg in self.associations[url]['direct'] or
                    msg.get('id') == candidate['message_id']):
                    continue
                # Use RAG backend's semantic search to judge association
                top_docs = rag_backend.semantic_search(text, n_results=3, channel=channel_name)
                if candidate['name'] in ''.join(top_docs):
                    self.associations[url]['fuzzy'].append(msg)

    @staticmethod
    def semantic_search_fn(message, candidate, rag_backend, threshold=0.75):
        """
        Returns True if the message is semantically similar to the candidate anchor.
        Uses cosine similarity between embeddings.
        """
        anchor_text = candidate['text']
        msg_text = message.get('text', '')
        if not msg_text.strip():
            return False
        anchor_emb = rag_backend.collection._embedding_function([anchor_text])[0]
        msg_emb = rag_backend.collection._embedding_function([msg_text])[0]
        sim = np.dot(anchor_emb, msg_emb) / (np.linalg.norm(anchor_emb) * np.linalg.norm(msg_emb))
        return sim >= threshold

    @staticmethod
    def llm_judge_fn(message, candidate, llm_client):
        """
        Returns True if the LLM says the message is about the candidate.
        llm_client must have an .ask(prompt) method that returns a string.
        """
        prompt = f"""
        Candidate: {candidate['name']} ({candidate['linkedin_url']})
        Message: {message.get('text', '')}
        Is this message about the candidate? Answer YES or NO.
        """
        response = llm_client.ask(prompt)
        return 'yes' in response.lower()

    def get_candidate_associations(self, linkedin_url: str) -> Optional[Dict]:
        return self.associations.get(linkedin_url)

    def get_all_candidates(self) -> List[Dict]:
        return self.candidates

    def get_all_associations(self) -> Dict[str, Dict]:
        return self.associations 