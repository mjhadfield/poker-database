import re
import json
import logging
from datetime import datetime
import pyodbc

class PokerHandParser:
    def __init__(self):
        # Initialize logging
        logging.basicConfig(
            filename='poker_hand_parser.log',
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # First check available SQL Server drivers
        self.available_drivers = [x for x in pyodbc.drivers() if 'SQL Server' in x]
        if not self.available_drivers:
            raise Exception("No SQL Server drivers found")
        
        # Use the most recent driver available
        self.driver = self.available_drivers[-1]
        logging.info(f"Using SQL Server driver: {self.driver}")
        
        # Connection strings with encryption settings
        self.master_conn_string = (
            f"DRIVER={{{self.driver}}};"
            "SERVER=localhost\\SQLEXPRESS;"
            "DATABASE=master;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
            "Encrypt=yes;"
        )
        
        self.db_conn_string = (
            f"DRIVER={{{self.driver}}};"
            "SERVER=localhost\\SQLEXPRESS;"
            "DATABASE=PokerHands;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
            "Encrypt=yes;"
        )

    def initialize_database(self):
        """Creates the PokerHands database if it doesn't exist"""
        try:
            # Connect to master database
            conn = pyodbc.connect(self.master_conn_string, autocommit=True)
            cursor = conn.cursor()
            
            # Check if database exists
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'PokerHands')
                BEGIN
                    CREATE DATABASE PokerHands
                END
            """)
            
            cursor.close()
            conn.close()
            
            logging.info("Database 'PokerHands' initialized successfully")
            
        except Exception as e:
            logging.error(f"Failed to initialize database: {str(e)}")
            raise Exception(f"Failed to initialize database: {str(e)}")
        
    def create_database_schema(self):
        """Creates the necessary database tables"""
        try:
            conn = pyodbc.connect(self.db_conn_string)
            cursor = conn.cursor()
            
            # Main hand history table
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'HandHistories')
                CREATE TABLE HandHistories (
                    HandId BIGINT PRIMARY KEY,
                    TournamentId BIGINT NULL,
                    GameType VARCHAR(50),
                    SmallBlind DECIMAL(10,2),
                    BigBlind DECIMAL(10,2),
                    PlayDateTime DATETIME,
                    PlayerCount INT,
                    ButtonSeat INT,
                    PlayerSeat INT,
                    HandPlayed CHAR(1),
                    Result DECIMAL(10,2),
                    RawHandHistory NVARCHAR(MAX),
                    JsonData NVARCHAR(MAX)
                )
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logging.info("Database schema created successfully")
            
        except Exception as e:
            logging.error(f"Failed to create database schema: {str(e)}")
            raise Exception(f"Failed to create database schema: {str(e)}")
        
    def parse_hand_history(self, hand_history_text):
        """Extracts key information from the hand history text."""
        try:
            # Extract hand number
            hand_number_match = re.search(r"PokerStars Hand #(\d+):", hand_history_text)
            hand_number = int(hand_number_match.group(1)) if hand_number_match else None
            if not hand_number:
                logging.warning("Hand number not found, skipping hand.")
                return None
            # Try to match cash game stakes (e.g., $0.01/$0.02 USD)
            cash_game_match = re.search(r"\(\$(\d+\.\d+)/\$(\d+\.\d+)", hand_history_text)
            if cash_game_match:
                small_blind = float(cash_game_match.group(1))
                big_blind = float(cash_game_match.group(2))
            else:
                # Try to match tournament blinds (e.g., 10/20 for Level I)
                tournament_match = re.search(r"Level \w+ \((\d+)/(\d+)\)", hand_history_text)
                if tournament_match:
                    small_blind = int(tournament_match.group(1))
                    big_blind = int(tournament_match.group(2))
                else:
                    small_blind = None
                    big_blind = None
            # Determine the game type
            if "Tournament" in hand_history_text:
                game_type = "Tournament"
            else:
                game_type = "Cash"
            # Determine if the hand was played
            if "everyonedoes folded before Flop (didn't bet)" in hand_history_text:
                hand_played = "N"
            elif "everyonedoes (button) folded before Flop (didn't bet)" in hand_history_text:
                hand_played = "N"
            else:
                hand_played = "Y"
            
            return {
                "HandId": hand_number,
                "SmallBlind": small_blind,
                "BigBlind": big_blind,
                "RawHandHistory": hand_history_text,
                "GameType": game_type,
                "HandPlayed": hand_played
            }
        except Exception as e:
            logging.error(f"Failed to parse hand history: {str(e)}")
            raise Exception(f"Failed to parse hand history: {str(e)}")

    def insert_hand_into_db(self, parsed_hand):
        """Inserts the parsed hand information into the database."""
        try:
            if not parsed_hand or parsed_hand['HandId'] is None:
                logging.error(f"Skipping hand with NULL HandId: {parsed_hand}")
                return

            conn = pyodbc.connect(self.db_conn_string)
            cursor = conn.cursor()
            
            # Insert parsed data into HandHistories table
            cursor.execute("""
                INSERT INTO HandHistories (HandId, SmallBlind, BigBlind, RawHandHistory, GameType, HandPlayed)
                VALUES (?, ?, ?, ?, ?, ?)
            """, 
            parsed_hand["HandId"], 
            parsed_hand["SmallBlind"], 
            parsed_hand["BigBlind"],
            parsed_hand["RawHandHistory"],
            parsed_hand["GameType"],
            parsed_hand["HandPlayed"])
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logging.info(f"Hand {parsed_hand['HandId']} inserted successfully.")
        except Exception as e:
            logging.error(f"Failed to insert hand {parsed_hand['HandId']} into database: {str(e)}")
            raise Exception(f"Failed to insert hand into database: {str(e)}")

    def process_hand_history_file(self, filename):
        """Reads hand history from a file and processes each hand."""
        try:
            with open(filename, "r", encoding="utf-8") as file:
                hand_history_text = file.read()
                
            # Split the hand histories based on three newlines
            hand_histories = hand_history_text.split("\n\n\n")
            
            # Process each hand history
            for idx, hand_history in enumerate(hand_histories, start=1):
                try:
                    logging.info(f"Processing hand {idx}...")
                    parsed_hand = self.parse_hand_history(hand_history)
                    if parsed_hand:
                        self.insert_hand_into_db(parsed_hand)
                except Exception as e:
                    logging.error(f"Error processing hand {idx}: {str(e)}")
                    continue
                
        except FileNotFoundError:
            logging.error(f"File '{filename}' not found.")
        except Exception as e:
            logging.error(f"Failed to process hand history file: {str(e)}")
            raise Exception(f"Failed to process hand history file: {str(e)}")

# Usage
if __name__ == "__main__":
    hand_parser = PokerHandParser()
    hand_parser.initialize_database()
    hand_parser.create_database_schema()
    
    # Process the hand history from a file
    hand_parser.process_hand_history_file("handhistory.txt")