import sys
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QTextEdit, QListWidget, QMessageBox, QLineEdit, QDateEdit, QFormLayout, QProgressDialog, QCheckBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QDate
from slack_analyzer_core import SlackAnalyzerCore
from config import load_config
import os
from datetime import datetime
from slack_rag_backend import SlackRAGBackend
from slack_analyzer import SlackDataStore
import re

class ClaudeThread(QThread):
    response_ready = pyqtSignal(str)
    def __init__(self, core, client_id, prompt):
        super().__init__()
        self.core = core
        self.client_id = client_id
        self.prompt = prompt
    def run(self):
        # Use Claude with RAG on the selected client
        response = self.core.claude_prompt(self.prompt, channel_id=self.client_id)
        self.response_ready.emit(response)

class ImportThread(QThread):
    status_update = pyqtSignal(str)
    finished = pyqtSignal()
    def __init__(self, core, client_ids, user_email, start_date, end_date):
        super().__init__()
        self.core = core
        self.client_ids = client_ids
        self.user_email = user_email
        self.start_date = start_date
        self.end_date = end_date
    def run(self):
        try:
            for client_id in self.client_ids:
                self.status_update.emit(f'Importing Slack data for client: {client_id}...')
                self.core.sync_with_api(
                    days=(self.end_date - self.start_date).days + 1,
                    status_callback=self.status_update.emit,
                    channel_id=client_id,
                    user_email=self.user_email,
                    start_date=self.start_date,
                    end_date=self.end_date
                )
        except Exception as e:
            self.status_update.emit(f'Error: {e}')
        self.finished.emit()

class SyncAndCacheThread(QThread):
    status_update = pyqtSignal(str)
    finished = pyqtSignal(int, int, bool)  # num_channels, num_messages, from_cache
    error = pyqtSignal(str)
    def __init__(self, core, user_email, start_date, end_date, force_refresh=False):
        super().__init__()
        self.core = core
        self.user_email = user_email
        self.start_date = start_date
        self.end_date = end_date
        self.force_refresh = force_refresh
    def run(self):
        try:
            self.status_update.emit('Checking cache...')
            # Call sync_with_api, which will use cache if available
            from_cache = False
            def status_cb(msg):
                if msg == 'Loaded from cache!':
                    nonlocal from_cache
                    from_cache = True
                self.status_update.emit(msg)
            # Modified to fetch ALL messages in the channels, not just user's messages
            self.core.sync_with_api(
                days=(self.end_date - self.start_date).days + 1,
                status_callback=status_cb,
                user_email=self.user_email,  # Still needed for auth but not for filtering messages
                start_date=self.start_date,
                end_date=self.end_date,
                force_refresh=self.force_refresh,
                fetch_all_messages=True  # New parameter to indicate we want all messages
            )
            # Count channels and messages for summary
            with self.core.conn:
                cur = self.core.conn.execute("SELECT COUNT(DISTINCT channel_id) FROM messages WHERE timestamp >= ? AND timestamp <= ?", (
                    datetime.combine(self.start_date, datetime.min.time()).timestamp(),
                    datetime.combine(self.end_date, datetime.max.time()).timestamp()
                ))
                num_channels = cur.fetchone()[0]
                cur = self.core.conn.execute("SELECT COUNT(*) FROM messages WHERE timestamp >= ? AND timestamp <= ?", (
                    datetime.combine(self.start_date, datetime.min.time()).timestamp(),
                    datetime.combine(self.end_date, datetime.max.time()).timestamp()
                ))
                num_messages = cur.fetchone()[0]
            self.finished.emit(num_channels, num_messages, from_cache)
        except Exception as e:
            self.error.emit(str(e))

class SlackAnalyzerApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Slack Claude RAG GUI')
        self.setGeometry(100, 100, 900, 650)
        app_config = load_config()
        api_config = app_config.api
        api_config.db_path = app_config.db.url.replace('sqlite:///', '') if app_config.db.url.startswith('sqlite:///') else app_config.db.url
        data_store = SlackDataStore(api_config.db_path)
        rag_backend = SlackRAGBackend(
            chroma_path=".chroma",
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            slack_token=os.getenv("SLACK_TOKEN"),
            data_store=data_store
        )
        print(f"[DEBUG] rag_backend initialized: {rag_backend is not None}")
        self.core = SlackAnalyzerCore(
            config=api_config,
            db_manager=None,
            cache=None,
            rate_limiter=None,
            candidate_extractor=CandidateExtractor(),
            rag_backend=rag_backend
        )
        self.clients = []
        self.selected_client_ids = []
        self.init_ui()
        self.sync_complete = False
        self.num_channels = 0
        self.num_messages = 0

    def init_ui(self):
        self.layout = QVBoxLayout(self)
        self.form_layout = QFormLayout()
        self.email_input = QLineEdit()
        self.email_input.setPlaceholderText('user@email.com (required)')
        self.email_input.setText('dkimball@candidatelabs.com')
        self.form_layout.addRow('User Email:', self.email_input)
        self.start_date = QDateEdit()
        self.start_date.setCalendarPopup(True)
        self.start_date.setDate(QDate.currentDate().addDays(-7))
        self.form_layout.addRow('Start Date:', self.start_date)
        self.end_date = QDateEdit()
        self.end_date.setCalendarPopup(True)
        self.end_date.setDate(QDate.currentDate())
        self.form_layout.addRow('End Date:', self.end_date)
        self.force_refresh_checkbox = QCheckBox('Force Refresh (ignore cache)')
        self.form_layout.addRow('', self.force_refresh_checkbox)
        self.layout.addLayout(self.form_layout)
        self.sync_button = QPushButton('Sync & Cache Slack Data')
        self.sync_button.clicked.connect(self.sync_and_cache)
        self.layout.addWidget(self.sync_button)
        self.status_label = QLabel('')
        self.layout.addWidget(self.status_label)
        # RAG/Claude UI (hidden until sync)
        self.rag_prompt_label = QLabel('Custom Claude Prompt:')
        self.rag_prompt = QTextEdit()
        self.rag_prompt.setPlainText(
            """Please act as DK's virtual recruiting assistant. Analyze the Slack channel data and prepare a comprehensive candidate pipeline report based on the messages below.\n\n**Instructions:**\n- For each channel (client), **group and list all candidates** and provide the latest understanding of their status in the process.\n- **Only include channels where dkimball@candidatelabs.com is a participant.**\n- For each candidate, use bullet points to summarize:\n  - Candidate name and LinkedIn URL (if available)\n  - Date of initial submission (MM/DD format)\n  - All feedback, updates, or reactions (with date and who gave the feedback)\n  - The most recent status or next step\n- **If a candidate has been submitted but received no client reaction or feedback, flag this as "Needs follow-up."**\n- Add the date (MM/DD) to each event or feedback for timeline clarity.\n- **Do not omit any candidate or channel that dkimball@candidatelabs.com is involved in.**\n\n**Format:**\n- Group by channel (client name as header)\n- For each candidate, use bullet points as described above\n- Be concise but thorough\n"""
        )
        self.ask_button = QPushButton('Ask Claude')
        self.ask_button.clicked.connect(self.ask_claude)
        self.rag_output_label = QLabel('Claude Output:')
        self.rag_output = QTextEdit()
        self.rag_output.setReadOnly(True)
        self.summary_label = QLabel('')
        self.rag_prompt_label.hide()
        self.rag_prompt.hide()
        self.ask_button.hide()
        self.rag_output_label.hide()
        self.rag_output.hide()
        self.summary_label.hide()
        self.layout.addWidget(self.summary_label)
        self.layout.addWidget(self.rag_prompt_label)
        self.layout.addWidget(self.rag_prompt)
        self.layout.addWidget(self.ask_button)
        self.layout.addWidget(self.rag_output_label)
        self.layout.addWidget(self.rag_output)
        self.setLayout(self.layout)

    def load_clients(self):
        try:
            self.clients = self.core.get_channels()
            self.client_list.clear()
            self.client_list.addItem('All Clients')
            for client in self.clients:
                self.client_list.addItem(client['name'])
            # Select all by default
            for i in range(self.client_list.count()):
                self.client_list.item(i).setSelected(True)
        except Exception as e:
            QMessageBox.critical(self, 'Error', f'Failed to load clients: {e}')

    def on_client_selected(self):
        selected_items = self.client_list.selectedItems()
        if not selected_items:
            self.selected_client_ids = []
            return
        names = [item.text() for item in selected_items]
        if 'All Clients' in names:
            # Select all client IDs
            self.selected_client_ids = [c['id'] for c in self.clients]
            # Select all in the UI
            for i in range(self.client_list.count()):
                self.client_list.item(i).setSelected(True)
        else:
            self.selected_client_ids = [c['id'] for c in self.clients if c['name'] in names]

    def sync_and_cache(self):
        print("[DEBUG] sync_and_cache called")
        user_email = self.email_input.text().strip()
        if not user_email:
            QMessageBox.warning(self, 'Missing Email', 'Please enter your email to sync.')
            return
        start_date = self.start_date.date().toPyDate()
        end_date = self.end_date.date().toPyDate()
        if start_date > end_date:
            QMessageBox.warning(self, 'Invalid Date Range', 'Start date must be before end date.')
            return
        force_refresh = self.force_refresh_checkbox.isChecked()
        self.status_label.setText('Starting sync...')
        self.sync_button.setEnabled(False)
        self.sync_thread = SyncAndCacheThread(self.core, user_email, start_date, end_date, force_refresh)
        self.sync_thread.status_update.connect(self.status_label.setText)
        self.sync_thread.finished.connect(self.on_sync_complete)
        self.sync_thread.error.connect(self.on_sync_error)
        self.sync_thread.start()

    def on_sync_complete(self, num_channels, num_messages, from_cache):
        self.sync_complete = True
        self.num_channels = num_channels
        self.num_messages = num_messages
        if from_cache:
            self.status_label.setText('Loaded from cache!')
        else:
            self.status_label.setText('Sync complete!')
        self.summary_label.setText(f'Cached {num_channels} channels, {num_messages} messages.')
        self.summary_label.show()
        self.rag_prompt_label.show()
        self.rag_prompt.show()
        self.ask_button.show()
        self.rag_output_label.show()
        self.rag_output.show()
        self.sync_button.setEnabled(True)

    def on_sync_error(self, error):
        self.status_label.setText(f'Error: {error}')
        self.sync_button.setEnabled(True)

    def ask_claude(self):
        if not self.sync_complete:
            QMessageBox.warning(self, 'Sync Required', 'Please sync and cache Slack data first.')
            return
        prompt = self.rag_prompt.toPlainText().strip()
        if not prompt:
            QMessageBox.warning(self, 'No Prompt', 'Please enter a prompt for Claude.')
            return
        self.rag_output.setPlainText('Asking Claude...')
        response = self.core.claude_prompt(prompt)
        print("[DEBUG] Context sent to Claude:\n", response)
        self.rag_output.setPlainText(response)

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

    def get_cached_messages(self, start_ts, end_ts, channel_id=None):
        # Implementation of get_cached_messages method
        pass

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SlackAnalyzerApp()
    window.show()
    sys.exit(app.exec())  # This keeps the window open until closed 