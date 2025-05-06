#!/usr/bin/env python3

import sys
import os
from datetime import datetime, timedelta
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                            QHBoxLayout, QLabel, QLineEdit, QPushButton, 
                            QDateEdit, QTextEdit, QMessageBox, QProgressBar)
from PyQt6.QtCore import Qt, QDate, QThread, pyqtSignal
from PyQt6.QtGui import QFont, QIcon
from generate_digest import SlackDigestGenerator
from dotenv import load_dotenv

DEFAULT_PROMPT = (
    "Please act as DK's virtual recruiting assistant. Analyze the Slack channel \"{channel_name}\" and prepare a comprehensive candidate pipeline report based on the messages below.\n\n"
    "Your primary tasks are to:\n"
    "1. Track ALL candidates in the pipeline for this company, regardless of when they were initially submitted\n"
    "2. Distinguish between new submissions and ongoing candidates\n"
    "3. Identify each candidate's current position in the hiring pipeline\n"
    "4. Flag candidates requiring follow-up, especially those with no recent updates\n\n"
    "For each candidate:\n"
    "- Note when they were initially submitted (if mentioned)\n"
    "- Track their current status in the hiring process\n"
    "- Highlight any recent feedback or updates from this reporting period\n"
    "- Flag candidates with no recent activity who require follow-up (over 1 week of no activity, mention)\n\n"
    "Create separate sections for:\n"
    "1. NEW SUBMISSIONS: Candidates newly submitted during this reporting period only\n"
    "2. ACTIVE PIPELINE: ALL candidates in process (including those submitted before this reporting period)\n"
    "   - With updates this week (highlight the new information)\n"
    "   - Without updates this week (note last known status and time since last update)\n"
    "3. FOLLOW-UP NEEDED: Candidates requiring immediate attention (no response, unclear status, etc.)\n"
    "4. ACTION ITEMS: Specific tasks that need attention with deadlines if applicable\n\n"
    "{linkedin_info}\n\n"
    "Channel messages:\n{messages_text}\n\n"
    "Format your response as a structured table with two columns:\n\n"
    "LEFT COLUMN: Company name (\"{channel_name}\")\n\n"
    "RIGHT COLUMN: Pipeline information organized as follows:\n"
    "1. PIPELINE SUMMARY (one-line overview with counts)\n"
    "   - Total candidates in pipeline\n"
    "   - New submissions this reporting period\n"
    "   - Candidates with updates this reporting period\n"
    "   - Candidates needing follow-up\n\n"
    "2. DETAILED SECTIONS (with clear headers):\n"
    "   - NEW SUBMISSIONS: Candidates newly submitted during this reporting period only\n"
    "   - ACTIVE PIPELINE: ALL candidates in process\n"
    "     * With updates this week (highlight the new information)\n"
    "     * Without updates this week (note last known status and time since last update)\n"
    "   - FOLLOW-UP NEEDED: Candidates requiring immediate attention\n"
    "   - ACTION ITEMS: Specific tasks with deadlines if applicable\n\n"
    "Use markdown table formatting for consistency. For example:\n\n"
    "| Company | Pipeline Status |\n"
    "|---------|----------------|\n"
    "| {channel_name} | **PIPELINE SUMMARY**: 12 total candidates \\| 3 new submissions \\| 5 with updates \\| 4 needing follow-up<br><br>**NEW SUBMISSIONS**:<br>• John Smith - Frontend Developer (submitted May 2)<br>• Jane Doe - Product Manager (submitted May 3)<br><br>**ACTIVE PIPELINE**:<br>• Alex Johnson - Interview scheduled May 8<br>• Sarah Williams - Awaiting feedback (2 weeks since last update)<br><br>**FOLLOW-UP NEEDED**:<br>• Michael Brown - No response for 3 weeks<br><br>**ACTION ITEMS**:<br>• Email hiring manager about Michael Brown by EOD |\n\n"
    "This report will help DK quickly understand the current candidate pipeline for each company and prioritize follow-up actions."
)

class DigestWorker(QThread):
    """Worker thread for generating the digest"""
    progress = pyqtSignal(str)
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)

    def __init__(self, token, user_email, start_date, end_date, timezone, custom_prompt=None):
        super().__init__()
        self.token = token
        self.user_email = user_email
        self.start_date = start_date
        self.end_date = end_date
        self.timezone = timezone
        self.custom_prompt = custom_prompt

    def run(self):
        try:
            generator = SlackDigestGenerator(self.token, self.user_email, self.timezone, self.custom_prompt)
            start_ts, end_ts = generator.get_date_range(self.start_date, self.end_date)
            
            self.progress.emit("Processing channels...")
            digest = generator.generate_digest(start_ts, end_ts)
            
            if digest:
                self.finished.emit(digest)
            else:
                self.error.emit("No activity found for the specified date range.")
        except Exception as e:
            self.error.emit(str(e))

class SlackDigestApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Slack Digest Generator")
        self.setMinimumSize(800, 600)
        
        # Load environment variables
        load_dotenv()
        
        # Create main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)
        
        # Create form fields
        form_layout = QVBoxLayout()
        
        # Email field
        email_layout = QHBoxLayout()
        email_label = QLabel("User Email:")
        self.email_input = QLineEdit()
        self.email_input.setPlaceholderText("Enter your Slack email")
        email_layout.addWidget(email_label)
        email_layout.addWidget(self.email_input)
        form_layout.addLayout(email_layout)
        
        # Date range fields
        date_layout = QHBoxLayout()
        
        # Start date
        start_date_label = QLabel("Start Date:")
        self.start_date = QDateEdit()
        self.start_date.setDate(QDate.currentDate().addDays(-7))
        self.start_date.setCalendarPopup(True)
        date_layout.addWidget(start_date_label)
        date_layout.addWidget(self.start_date)
        
        # End date
        end_date_label = QLabel("End Date:")
        self.end_date = QDateEdit()
        self.end_date.setDate(QDate.currentDate())
        self.end_date.setCalendarPopup(True)
        date_layout.addWidget(end_date_label)
        date_layout.addWidget(self.end_date)
        
        form_layout.addLayout(date_layout)
        
        # Timezone field
        timezone_layout = QHBoxLayout()
        timezone_label = QLabel("Timezone:")
        self.timezone_input = QLineEdit("America/Chicago")
        timezone_layout.addWidget(timezone_label)
        timezone_layout.addWidget(self.timezone_input)
        form_layout.addLayout(timezone_layout)
        
        # Custom Prompt field
        prompt_label = QLabel("Custom Prompt (optional):")
        self.prompt_input = QTextEdit()
        self.prompt_input.setPlainText(DEFAULT_PROMPT)
        self.prompt_input.setFont(QFont("Courier", 9))
        form_layout.addWidget(prompt_label)
        form_layout.addWidget(self.prompt_input)
        
        # Generate button
        self.generate_button = QPushButton("Generate Digest")
        self.generate_button.clicked.connect(self.generate_digest)
        form_layout.addWidget(self.generate_button)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setAlignment(Qt.AlignmentFlag.AlignCenter)
        form_layout.addWidget(self.progress_bar)
        
        # Output area
        self.output_area = QTextEdit()
        self.output_area.setReadOnly(True)
        self.output_area.setFont(QFont("Courier", 10))
        
        # Add layouts to main layout
        layout.addLayout(form_layout)
        layout.addWidget(self.output_area)
        
        # Set initial state
        self.progress_bar.hide()
        
        # Check for required environment variables
        if not os.getenv("SLACK_USER_TOKEN"):
            QMessageBox.warning(self, "Configuration Error", 
                              "SLACK_USER_TOKEN environment variable not set!")
        if not os.getenv("ANTHROPIC_API_KEY"):
            QMessageBox.warning(self, "Configuration Error", 
                              "ANTHROPIC_API_KEY environment variable not set!")

    def generate_digest(self):
        # Get input values
        user_email = self.email_input.text().strip()
        start_date = self.start_date.date().toString("yyyy-MM-dd")
        end_date = self.end_date.date().toString("yyyy-MM-dd")
        timezone = self.timezone_input.text().strip()
        custom_prompt = self.prompt_input.toPlainText().strip()
        
        # Validate inputs
        if not user_email:
            QMessageBox.warning(self, "Input Error", "Please enter your Slack email.")
            return
        
        # Get token from environment
        token = os.getenv("SLACK_USER_TOKEN")
        if not token:
            QMessageBox.critical(self, "Error", "SLACK_USER_TOKEN not found in environment variables.")
            return
        
        # Disable generate button and show progress
        self.generate_button.setEnabled(False)
        self.progress_bar.setValue(0)
        self.progress_bar.show()
        self.output_area.clear()
        
        # If the prompt box is empty, use the default
        if not custom_prompt:
            custom_prompt = DEFAULT_PROMPT
        
        # Create and start worker thread
        self.worker = DigestWorker(token, user_email, start_date, end_date, timezone, custom_prompt)
        self.worker.progress.connect(self.update_progress)
        self.worker.finished.connect(self.handle_digest)
        self.worker.error.connect(self.handle_error)
        self.worker.start()

    def update_progress(self, message):
        self.progress_bar.setValue(50)
        self.output_area.append(f"Status: {message}")

    def handle_digest(self, digest):
        self.progress_bar.setValue(100)
        self.output_area.clear()
        
        # Format and display the digest
        for channel_name, summary in digest.items():
            self.output_area.append(f"## {channel_name}\n")
            self.output_area.append(summary)
            self.output_area.append("\n---\n")
        
        # Save to file
        start_date = self.start_date.date().toString("yyyy-MM-dd")
        end_date = self.end_date.date().toString("yyyy-MM-dd")
        output_file = f"client_digest_{start_date}_to_{end_date}.md"
        
        try:
            with open(output_file, 'w') as f:
                f.write(self.output_area.toPlainText())
            QMessageBox.information(self, "Success", 
                                  f"Digest saved to {output_file}")
        except Exception as e:
            QMessageBox.warning(self, "Warning", 
                              f"Digest generated but could not save to file: {str(e)}")
        
        # Reset UI
        self.generate_button.setEnabled(True)
        self.progress_bar.hide()

    def handle_error(self, error_message):
        self.progress_bar.hide()
        self.generate_button.setEnabled(True)
        QMessageBox.critical(self, "Error", error_message)

def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')  # Use Fusion style for a modern look
    
    # Set application icon if available
    if os.path.exists("icon.png"):
        app.setWindowIcon(QIcon("icon.png"))
    
    window = SlackDigestApp()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main() 