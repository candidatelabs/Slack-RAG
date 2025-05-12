import sys
import os
from pathlib import Path
from PyQt6.QtWidgets import QApplication, QMainWindow, QMessageBox
from PyQt6.QtCore import Qt

from config import load_config
from logger import setup_logging, get_logger, start_metrics_server
from database import DatabaseManager
from cache import PersistentCache
from rate_limiter import RateLimiter
from slack_analyzer_core import SlackAnalyzerCore
from slack_rag_backend import SlackRAGBackend
from candidate_extractor import CandidateExtractor
from ui.main_window import MainWindow

def setup_environment() -> None:
    """Setup the application environment."""
    # Create necessary directories
    Path(".cache").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)

    # Load configuration
    config = load_config()

    # Setup logging
    setup_logging(config.log_level)
    logger = get_logger(__name__)
    logger.info("Application starting", config=config)

    # Start metrics server
    start_metrics_server()

    return config

def main() -> None:
    """Main application entry point."""
    try:
        # Setup environment
        config = setup_environment()
        logger = get_logger(__name__)

        # Initialize components
        db_manager = DatabaseManager(config.db)
        cache = PersistentCache(config.cache)
        rate_limiter = RateLimiter(config.api)
        
        # Create database tables
        db_manager.create_tables()

        # Initialize core components
        rag_backend = SlackRAGBackend(config.api)
        candidate_extractor = CandidateExtractor(rag_backend)
        analyzer_core = SlackAnalyzerCore(
            config.api,
            db_manager,
            cache,
            rate_limiter,
            candidate_extractor,
            rag_backend
        )

        # Create and show the main window
        app = QApplication(sys.argv)
        window = MainWindow(analyzer_core)
        window.show()

        # Start the event loop
        sys.exit(app.exec())

    except Exception as e:
        logger.error("Application failed to start", error=str(e))
        QMessageBox.critical(None, "Error", f"Failed to start application: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 