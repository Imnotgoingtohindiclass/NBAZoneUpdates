import logging
import os
import datetime
import sqlite3
from dotenv import load_dotenv
import pandas as pd
import pytz # Required for timezone handling: pip install pytz

from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes, ApplicationBuilder
from telegram.error import Forbidden, BadRequest # For handling send errors

# --- NBA API Imports ---
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import (
    playercareerstats,
    playergamelog,
    commonteamroster,
    leaguegamefinder,
    leaguestandingsv3,
    leaguedashteamstats,
    commonplayerinfo
)

# --- Configuration ---
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CURRENT_SEASON = '2024-25'
DB_FILE = 'nba_bot_data.db'
NBA_TZ = pytz.timezone("Asia/Singapore")

# --- Logging Setup ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Database Functions ---

def init_db():
    """Initializes the database and creates tables if they don't exist."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
            # Check if tables exist with the correct structure
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_player_follows'")
            table_exists = cursor.fetchone() is not None
            
            if table_exists:
                # Check if the table has the correct structure
                cursor.execute("PRAGMA table_info(user_player_follows)")
                columns = [column[1] for column in cursor.fetchall()]
                
                # If player_id column is missing, drop and recreate the tables
                if 'player_id' not in columns:
                    logger.info("Database exists but has incorrect structure. Recreating tables...")
                    cursor.execute("DROP TABLE IF EXISTS user_player_follows")
                    cursor.execute("DROP TABLE IF EXISTS sent_notifications")
                    table_exists = False
            
            # Create tables if they don't exist
            if not table_exists:
                # Create user_player_follows table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS user_player_follows (
                        chat_id INTEGER NOT NULL,
                        player_id INTEGER NOT NULL,
                        player_full_name TEXT NOT NULL COLLATE NOCASE,
                        PRIMARY KEY (chat_id, player_id)
                    )
                ''')
                # Add index for faster lookup by player_id in jobs
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_player_id ON user_player_follows (player_id)
                ''')

                # Create table to track sent notifications
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS sent_notifications (
                        chat_id INTEGER NOT NULL,
                        player_id INTEGER NOT NULL,
                        game_id TEXT NOT NULL,
                        notification_type TEXT NOT NULL, -- 'upcoming' or 'finished'
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (chat_id, player_id, game_id, notification_type)
                    )
                ''')
            
            conn.commit()
            logger.info(f"Database {DB_FILE} initialized/updated successfully.")
    except sqlite3.Error as e:
        logger.error(f"Database initialization error: {e}")
        raise

def add_follow(chat_id: int, player_id: int, player_name: str) -> bool:
    """Adds a player (with ID) to a user's follow list. Returns True if added."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO user_player_follows (chat_id, player_id, player_full_name)
                VALUES (?, ?, ?)
            ''', (chat_id, player_id, player_name))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"Error adding follow for chat {chat_id}, player ID {player_id}: {e}")
        return False

def remove_follow(chat_id: int, player_name: str) -> bool:
    """Removes a player from a user's follow list by name. Returns True if removed."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            # Note: Still using player_full_name for deletion as provided by user
            cursor.execute('''
                DELETE FROM user_player_follows
                WHERE chat_id = ? AND player_full_name = ?
            ''', (chat_id, player_name))
            conn.commit()
            # Also clean up any sent notifications for this user/player combo if unfollowed
            # This is optional but good practice
            if cursor.rowcount > 0:
                cursor.execute('''
                    DELETE FROM sent_notifications
                    WHERE chat_id = ? AND player_id = (
                        SELECT player_id FROM user_player_follows WHERE chat_id = ? AND player_full_name = ? LIMIT 1
                    )
                ''', (chat_id, chat_id, player_name)) # Re-querying player_id is needed here or pass it
                conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"Error removing follow for chat {chat_id}, player {player_name}: {e}")
        return False

def get_followed_players(chat_id: int) -> list[tuple[int, str]]:
    """Retrieves the list of (player_id, player_full_name) a user follows."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT player_id, player_full_name FROM user_player_follows
                WHERE chat_id = ?
                ORDER BY player_full_name COLLATE NOCASE ASC
            ''', (chat_id,))
            followed = cursor.fetchall() # Returns list of tuples [(id, name), ...]
            return followed
    except sqlite3.Error as e:
        logger.error(f"Error fetching followed players for chat {chat_id}: {e}")
        return []
    
def get_all_follows() -> dict[int, list[int]]:
    """Retrieves all player follows, mapping player_id to list of chat_ids."""
    follows = {}
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            # Get distinct player_ids first for efficiency maybe? No, easier to group later.
            cursor.execute('SELECT player_id, chat_id FROM user_player_follows')
            for player_id, chat_id in cursor.fetchall():
                if player_id not in follows:
                    follows[player_id] = []
                if chat_id not in follows[player_id]: # Avoid duplicates if DB somehow has them
                    follows[player_id].append(chat_id)
            return follows
    except sqlite3.Error as e:
        logger.error(f"Error fetching all follows: {e}")
        return {}
    

# New functions to check/mark sent notifications
def has_notification_been_sent(chat_id: int, player_id: int, game_id: str, notification_type: str) -> bool:
    """Checks if a specific notification has been sent."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 1 FROM sent_notifications
                WHERE chat_id = ? AND player_id = ? AND game_id = ? AND notification_type = ?
            ''', (chat_id, player_id, game_id, notification_type))
            return cursor.fetchone() is not None
    except sqlite3.Error as e:
        logger.error(f"Error checking sent notification: {e}")
        return False # Assume not sent on error to be safe? Or True? Needs thought.

def mark_notification_sent(chat_id: int, player_id: int, game_id: str, notification_type: str):
    """Marks a notification as sent."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO sent_notifications (chat_id, player_id, game_id, notification_type)
                VALUES (?, ?, ?, ?)
            ''', (chat_id, player_id, game_id, notification_type))
            conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error marking notification sent: {e}")

async def check_upcoming_games(context: ContextTypes.DEFAULT_TYPE):
    """Checks for games happening tomorrow and notifies followed players."""
    logger.info("Running job: check_upcoming_games")
    now_et = datetime.datetime.now(NBA_TZ)
    tomorrow_et_start = (now_et + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    day_after_tomorrow_et_start = (now_et + datetime.timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)

    logger.info(f"Checking for games between {tomorrow_et_start} and {day_after_tomorrow_et_start}")

    # 1. Get all follows (player_id -> list[chat_id])
    all_follows = get_all_follows()
    if not all_follows:
        logger.info("No players being followed. Skipping upcoming game check.")
        return

    # 2. Fetch relevant games (e.g., next few days)
    # Note: LeagueGameFinder might not be the *most* efficient way, but it's available
    # Filtering by date isn't directly supported in the params AFAIK, so we fetch recent/future
    try:
        finder = leaguegamefinder.LeagueGameFinder(league_id_nullable='00') # '00' for NBA
        all_games_df = finder.get_data_frames()[0]
        if all_games_df.empty:
            logger.warning("LeagueGameFinder returned no games.")
            return

        # Convert game dates to aware datetime objects in NBA timezone
        # Handle potential variations in GAME_DATE format if necessary
        try:
            all_games_df['GAME_DATETIME'] = pd.to_datetime(all_games_df['GAME_DATE'], errors='coerce').dt.tz_localize(NBA_TZ) # Assume dates are ET
        except Exception as date_err:
            logger.error(f"Could not parse GAME_DATE with timezone: {date_err}")
            # Try another format or skip
            try:
                # Example: If date is like 'APR 08, 2025'
                all_games_df['GAME_DATETIME'] = pd.to_datetime(all_games_df['GAME_DATE'], format='%b %d, %Y', errors='coerce').dt.tz_localize(NBA_TZ)
            except Exception as date_err_2:
                logger.error(f"Could not parse GAME_DATE with alternate format: {date_err_2}")
                return # Cannot proceed without dates

        # Filter games happening "tomorrow" relative to NBA_TZ
        upcoming_games_df = all_games_df[
            (all_games_df['GAME_DATETIME'] >= tomorrow_et_start) &
            (all_games_df['GAME_DATETIME'] < day_after_tomorrow_et_start)
        ].copy() # Use .copy() to avoid SettingWithCopyWarning

        logger.info(f"Found {len(upcoming_games_df)} games scheduled for tomorrow ({tomorrow_et_start.date()}).")

    except Exception as e:
        logger.error(f"Error fetching or processing game schedule: {e}")
        return

    # 3. Check each game against followed players
    processed_players_for_job = set() # Optimization: Process each player once per job run

    for index, game in upcoming_games_df.iterrows():
        game_id = game['GAME_ID']
        matchup = game['MATCHUP'] # e.g., 'LAL @ GSW' or 'LAL vs. GSW'
        team_ids = [game['TEAM_ID']] # The primary team_id in the row
        # TODO: Need a reliable way to get BOTH team IDs from the matchup string or game data.
        # This is a limitation of LeagueGameFinder's structure per row.
        # A potential workaround is to find the opponent ID from another row with the same game_id
        opponent_row = all_games_df[(all_games_df['GAME_ID'] == game_id) & (all_games_df['TEAM_ID'] != game['TEAM_ID'])]
        if not opponent_row.empty:
            opponent_id = opponent_row.iloc[0]['TEAM_ID']
            team_ids.append(opponent_id)
            opponent_abbr = opponent_row.iloc[0]['TEAM_ABBREVIATION']
            home_away_indicator = '@' if '@' in matchup else 'vs.'
            if game['TEAM_ABBREVIATION'] in matchup.split(home_away_indicator)[0]: # This team is listed first
                opponent_team_name = opponent_abbr # Simplified name
            else:
                opponent_team_name = matchup.split(home_away_indicator)[0].strip() # Try to get from matchup string

            # A better approach might involve getting team name from opponent_id using teams.find_team_by_id
            opponent_info = teams.find_team_by_id(opponent_id)
            if opponent_info:
                opponent_team_name = opponent_info['nickname'] # Or full_name

        else:
            opponent_team_name = "opponent" # Fallback

        # Check players on BOTH teams involved in the game
        # This requires mapping player_id to team_id, which we don't have directly stored.
        # Option A: Call CommonTeamRoster for both teams (API intensive!)
        # Option B: Assume player is on their 'current' team (needs lookup) - Less reliable due to trades/injuries near game time
        # Option C: Iterate through all followed players and check if *their team* is playing (more efficient)

        # Let's try Option C (Iterate through follows):
        for player_id, chat_ids in all_follows.items():
            if player_id in processed_players_for_job:
                 continue # Already notified (or checked) this player for *some* game tomorrow

            try:
                # Find the player's current team - requires an API call per player!
                # This is potentially slow and API-heavy. Consider caching.
                player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
                player_df = player_info.get_data_frames()[0]
                if player_df.empty:
                    logger.warning(f"Could not get info for player ID {player_id}")
                    continue
                current_team_id = player_df.iloc[0]['TEAM_ID']
                player_full_name = player_df.iloc[0]['DISPLAY_FIRST_LAST'] # Get canonical name

                # If this player's current team is in the game:
                if current_team_id in team_ids:
                    game_date_str = game['GAME_DATETIME'].strftime('%b %d, %Y')
                     # Determine opponent for *this* player
                    player_opponent_id = opponent_id if current_team_id == game['TEAM_ID'] else game['TEAM_ID']
                    opponent_info = teams.find_team_by_id(player_opponent_id)
                    opponent_name = opponent_info['nickname'] if opponent_info else 'opponent'
                    matchup_desc = f"vs {opponent_name}" if home_away_indicator == 'vs.' else f"@ {opponent_name}"


                    # Notify all users following this player
                    for chat_id in chat_ids:
                        if not has_notification_been_sent(chat_id, player_id, game_id, 'upcoming'):
                            message = (f"🔔 Game Tomorrow ({game_date_str})!\n\n"
                                        f"{player_full_name} has a game {matchup_desc}.")
                            try:
                                await context.bot.send_message(chat_id=chat_id, text=message)
                                mark_notification_sent(chat_id, player_id, game_id, 'upcoming')
                                logger.info(f"Sent upcoming game notification to {chat_id} for player {player_id}, game {game_id}")
                            except (Forbidden, BadRequest) as send_err:
                                logger.warning(f"Failed to send upcoming notification to {chat_id}: {send_err} - User might have blocked the bot.")
                                # Optional: Remove follow if Forbidden? Or just log.
                            except Exception as e:
                                logger.error(f"Error sending upcoming notification to {chat_id}: {e}")
                    processed_players_for_job.add(player_id) # Mark player processed for this job run

            except Exception as player_err:
                logger.error(f"Error processing player ID {player_id} for upcoming games: {player_err}")
                # Avoid adding to processed_players_for_job if error occurs before check

    logger.info("Finished job: check_upcoming_games")

async def check_finished_games(context: ContextTypes.DEFAULT_TYPE):
    """Checks for games finished yesterday and sends stats to followers."""
    logger.info("Running job: check_finished_games")
    now_et = datetime.datetime.now(NBA_TZ)
    yesterday_et = (now_et - datetime.timedelta(days=1)).date()

    logger.info(f"Checking for games finished on {yesterday_et}")

    # 1. Get all follows (player_id -> list[chat_id])
    all_follows = get_all_follows()
    if not all_follows:
        logger.info("No players being followed. Skipping finished game check.")
        return

    # 2. Fetch games from yesterday
    # Using LeagueGameFinder again, filtering needed
    try:
        finder = leaguegamefinder.LeagueGameFinder(league_id_nullable='00')
        all_games_df = finder.get_data_frames()[0]
        if all_games_df.empty:
            logger.warning("LeagueGameFinder returned no games for finished check.")
            return

        # Convert game dates and filter for yesterday
        try:
            # Try multiple formats if needed
            all_games_df['GAME_DATETIME'] = pd.to_datetime(all_games_df['GAME_DATE'], errors='coerce').dt.tz_localize(NBA_TZ)
        except Exception:
            try:
                all_games_df['GAME_DATETIME'] = pd.to_datetime(all_games_df['GAME_DATE'], format='%b %d, %Y', errors='coerce').dt.tz_localize(NBA_TZ)
            except Exception as date_err:
                logger.error(f"Could not parse GAME_DATE for finished games: {date_err}")
                return

        # Keep only games played yesterday, ensure WL column exists (indicates completed)
        finished_games_df = all_games_df[
            (all_games_df['GAME_DATETIME'].dt.date == yesterday_et) &
            (all_games_df['WL'].notna()) # Check if result is recorded
        ].copy()

        logger.info(f"Found {len(finished_games_df)} potential finished game records from {yesterday_et}.")
        # Note: Each game appears twice (once per team)

    except Exception as e:
        logger.error(f"Error fetching or processing game schedule for finished check: {e}")
        return

    # 3. Check each finished game against followed players
    processed_games_for_player = {} # player_id -> set(game_id) to avoid duplicate checks per player per game

    for player_id, chat_ids in all_follows.items():
        # Get player's game log for yesterday's date range
        try:
            # PlayerGameLog needs season, let's derive it (simple approach)
            # TODO: This needs a more robust way to handle season transitions
            season_year = yesterday_et.year if yesterday_et.month >= 10 else yesterday_et.year - 1
            season_str = f"{season_year}-{str(season_year+1)[-2:]}"

            gamelog = playergamelog.PlayerGameLog(
                player_id=player_id,
                season=season_str,
                date_from_nullable=yesterday_et.strftime('%m/%d/%Y'),
                date_to_nullable=yesterday_et.strftime('%m/%d/%Y')
            )
            log_df = gamelog.get_data_frames()[0]

            if log_df.empty:
                #logger.info(f"Player {player_id} had no game log for {yesterday_et}")
                continue # Player didn't play or log not updated yet

            # Should only be one game if date range is one day
            if len(log_df) > 1:
                logger.warning(f"Player {player_id} had multiple game logs for {yesterday_et}. Using the first.")

            last_game = log_df.iloc[0]
            game_id = last_game['Game_ID'] # Use the Game_ID from the log
            player_full_name = last_game['PLAYER_NAME'] # Get name from log

            # Check if we've already processed this specific game for this player
            if player_id in processed_games_for_player and game_id in processed_games_for_player[player_id]:
                continue

            # Format stats
            game_date = last_game['GAME_DATE']
            matchup = last_game['MATCHUP']
            wl = last_game['WL']
            pts = last_game['PTS']
            reb = last_game['REB']
            ast = last_game['AST']
            # ... (include other relevant stats like in /lastgame)
            fgm = last_game['FGM']
            fga = last_game['FGA']
            fg_pct = (fgm / fga * 100) if fga > 0 else 0
            # ... etc ...

            stats_message = (
                    f"📊 **{player_full_name} - Game Stats ({game_date})**\n"
                    f"Matchup: {matchup} ({wl})\n\n"
                    f"PTS: {pts} | REB: {reb} | AST: {ast}\n"
                    f"FG: {fgm}/{fga} ({fg_pct:.1f}%)\n"
                    # ... add more stats ...
                )
            for chat_id in chat_ids:
                if not has_notification_been_sent(chat_id, player_id, game_id, 'finished'):
                    try:
                        await context.bot.send_message(chat_id=chat_id, text=stats_message, parse_mode='Markdown')
                        mark_notification_sent(chat_id, player_id, game_id, 'finished')
                        logger.info(f"Sent finished game stats to {chat_id} for player {player_id}, game {game_id}")
                    except (Forbidden, BadRequest) as send_err:
                        logger.warning(f"Failed to send finished stats to {chat_id}: {send_err}")
                    except Exception as e:
                        logger.error(f"Error sending finished stats to {chat_id}: {e}")

            # Mark game as processed for this player
            if player_id not in processed_games_for_player:
                processed_games_for_player[player_id] = set()
            processed_games_for_player[player_id].add(game_id)

        except Exception as e:
            logger.error(f"Error processing finished games/stats for player ID {player_id}: {e}")
            import traceback
            traceback.print_exc() # More detail for debugging

    logger.info("Finished job: check_finished_games")

# --- Helper Functions (NBA API - unchanged) ---

async def find_player(player_name_query: str) -> list | None:
    """Finds players matching the query."""
    try:
        player_list = players.find_players_by_full_name(player_name_query)
        if not player_list:
            player_list = players.find_players_by_first_name(player_name_query)
        if not player_list:
            player_list = players.find_players_by_last_name(player_name_query)
        return player_list
    except Exception as e:
        logger.error(f"Error finding player '{player_name_query}': {e}")
        return None

async def find_team(team_name_query: str) -> list | None:
    """Finds teams matching the query."""
    try:
        team_list = teams.find_teams_by_full_name(team_name_query)
        if not team_list:
            team_list = teams.find_teams_by_nickname(team_name_query)
        if not team_list:
            team_list = teams.find_teams_by_city(team_name_query)
        if not team_list:
            team_list = teams.find_teams_by_abbreviation(team_name_query)
        return team_list
    except Exception as e:
        logger.error(f"Error finding team '{team_name_query}': {e}")
        return None

def get_season_string() -> str:
    """Gets the current NBA season string (e.g., 2024-25)."""
    # Using constant for reliability
    return CURRENT_SEASON

# --- Command Handlers (NBA API ones - unchanged) ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends a welcome message and lists commands."""
    user_name = update.effective_user.first_name
    help_text = (
        f"👋 Welcome to NBAZoneBot, {user_name}!\n\n"
        "Here's what I can do:\n\n"
        "**Players:**\n"
        "  `/playerstats [player_name]` - Get current season stats.\n"
        "  `/lastgame [player_name]` - Get stats from the player's most recent game.\n\n"
        "**Teams:**\n"
        "  `/teamroster [team_name]` - Show the team's current roster.\n"
        "  `/teamstats [team_name]` - Get current season team stats.\n"
        "  `/nextgame [team_name]` - Show the team's next scheduled game (experimental).\n\n"
        "**League:**\n"
        "  `/standings` - Get current league standings.\n\n"
        "**Following:**\n"
        "  `/follow [player_name]` - Start following a player.\n"
        "  `/unfollow [player_name]` - Stop following a player.\n"
        "  `/following` - List players you follow.\n\n"
        "Use `/help` to see this message again."
    )
    await update.message.reply_text(help_text, parse_mode='Markdown')


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends the help message."""
    await start(update, context)


async def player_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fetches and displays current season player stats."""
    if not context.args:
        await update.message.reply_text("Please provide a player name.\nUsage: `/playerstats [player_name]`")
        return

    player_name_query = " ".join(context.args)
    logger.info(f"Received /playerstats request for: {player_name_query}")

    found_players = await find_player(player_name_query)

    if not found_players:
        await update.message.reply_text(f"Sorry, I couldn't find a player named '{player_name_query}'.")
        return
    if len(found_players) > 1:
        await update.message.reply_text(f"Found multiple players for '{player_name_query}'. Please be more specific.\n"
                                        f"Matches: {[p['full_name'] for p in found_players[:5]]}")
        return

    player_info = found_players[0]
    player_id = player_info['id']
    player_full_name = player_info['full_name']
    current_season = get_season_string()

    try:
        career = playercareerstats.PlayerCareerStats(player_id=player_id, per_mode36='PerGame')
        stats_df = career.get_data_frames()[0]
        season_stats = stats_df[stats_df['SEASON_ID'] == current_season]

        if season_stats.empty:
            await update.message.reply_text(f"{player_full_name} has no stats recorded for the {current_season} season yet.")
            return

        stats = season_stats.iloc[0]
        ppg = stats['PTS']
        rpg = stats['REB']
        apg = stats['AST']
        fg_pct = stats['FG_PCT'] * 100 if pd.notna(stats['FG_PCT']) else 0
        fg3_pct = stats['FG3_PCT'] * 100 if pd.notna(stats['FG3_PCT']) else 0
        ft_pct = stats['FT_PCT'] * 100 if pd.notna(stats['FT_PCT']) else 0
        games_played = stats['GP']

        message = (
            f"🏀 **{player_full_name} ({current_season} Season Stats)**\n\n"
            f"Games Played: {games_played}\n"
            f"Points: {ppg:.1f} PPG\n"
            f"Rebounds: {rpg:.1f} RPG\n"
            f"Assists: {apg:.1f} APG\n"
            f"FG%: {fg_pct:.1f}%\n"
            f"3P%: {fg3_pct:.1f}%\n"
            f"FT%: {ft_pct:.1f}%"
        )
        await update.message.reply_text(message, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Error fetching stats for player ID {player_id} ({player_full_name}): {e}")
        await update.message.reply_text(f"Sorry, an error occurred while fetching stats for {player_full_name}.")


async def last_game_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fetches and displays player stats from their most recent game."""
    if not context.args:
        await update.message.reply_text("Please provide a player name.\nUsage: `/lastgame [player_name]`")
        return

    player_name_query = " ".join(context.args)
    logger.info(f"Received /lastgame request for: {player_name_query}")

    found_players = await find_player(player_name_query)

    if not found_players:
        await update.message.reply_text(f"Sorry, I couldn't find a player named '{player_name_query}'.")
        return
    if len(found_players) > 1:
        await update.message.reply_text(f"Found multiple players for '{player_name_query}'. Please be more specific.\n"
                                        f"Matches: {[p['full_name'] for p in found_players[:5]]}")
        return

    player_info = found_players[0]
    player_id = player_info['id']
    player_full_name = player_info['full_name']

    try:
        current_season = get_season_string()
        gamelog = playergamelog.PlayerGameLog(player_id=player_id, season=current_season)
        log_df = gamelog.get_data_frames()[0]

        if log_df.empty:
            await update.message.reply_text(f"{player_full_name} has no game logs available.")
            return

        last_game = log_df.iloc[0]
        game_date = last_game['GAME_DATE']
        matchup = last_game['MATCHUP']
        wl = last_game['WL']
        minutes = last_game['MIN']
        pts = last_game['PTS']
        reb = last_game['REB']
        ast = last_game['AST']
        stl = last_game['STL']
        blk = last_game['BLK']
        fgm = last_game['FGM']
        fga = last_game['FGA']
        fg3m = last_game['FG3M']
        fg3a = last_game['FG3A']
        ftm = last_game['FTM']
        fta = last_game['FTA']
        fg_pct = (fgm / fga * 100) if fga > 0 else 0
        fg3_pct = (fg3m / fg3a * 100) if fg3a > 0 else 0
        ft_pct = (ftm / fta * 100) if fta > 0 else 0

        message = (
            f"**{player_full_name} - Last Game**\n"
            f"Date: {game_date}\n"
            f"Matchup: {matchup} ({wl})\n\n"
            f"MIN: {minutes}\n"
            f"PTS: {pts}\n"
            f"REB: {reb}\n"
            f"AST: {ast}\n"
            f"STL: {stl}\n"
            f"BLK: {blk}\n"
            f"FG: {fgm}/{fga} ({fg_pct:.1f}%)\n"
            f"3PT: {fg3m}/{fg3a} ({fg3_pct:.1f}%)\n"
            f"FT: {ftm}/{fta} ({ft_pct:.1f}%)"
        )
        await update.message.reply_text(message, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Error fetching game log for player ID {player_id} ({player_full_name}): {e}")
        await update.message.reply_text(f"Sorry, an error occurred while fetching the last game stats for {player_full_name}.")


async def team_roster_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fetches and displays the current roster for a team."""
    if not context.args:
        await update.message.reply_text("Please provide a team name.\nUsage: `/teamroster [team_name]`")
        return

    team_name_query = " ".join(context.args)
    logger.info(f"Received /teamroster request for: {team_name_query}")

    found_teams = await find_team(team_name_query)

    if not found_teams:
        await update.message.reply_text(f"Sorry, I couldn't find a team matching '{team_name_query}'.")
        return
    if len(found_teams) > 1:
        await update.message.reply_text(f"Found multiple teams for '{team_name_query}'. Please be more specific.\n"
                                        f"Matches: {[t['full_name'] for t in found_teams[:5]]}")
        return

    team_info = found_teams[0]
    team_id = team_info['id']
    team_full_name = team_info['full_name']
    current_season = get_season_string()

    try:
        roster = commonteamroster.CommonTeamRoster(team_id=team_id, season=current_season)
        roster_df = roster.get_data_frames()[0]

        if roster_df.empty:
            await update.message.reply_text(f"Could not retrieve the {current_season} roster for the {team_full_name}.")
            return

        roster_list = []
        for index, player in roster_df.iterrows():
            num = player['NUM'] if pd.notna(player['NUM']) else '-'
            name = player['PLAYER']
            pos = player['POSITION'] if pd.notna(player['POSITION']) else '-'
            roster_list.append(f"#{num} {name} ({pos})")

        message = f"**{team_full_name} Roster ({current_season})**\n\n" + "\n".join(roster_list)
        if len(message) > 4096:
            message = message[:4090] + "\n..."
        await update.message.reply_text(message, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Error fetching roster for team ID {team_id} ({team_full_name}): {e}")
        await update.message.reply_text(f"Sorry, an error occurred while fetching the roster for {team_full_name}.")


async def team_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fetches and displays current season team stats."""
    if not context.args:
        await update.message.reply_text("Please provide a team name.\nUsage: `/teamstats [team_name]`")
        return

    team_name_query = " ".join(context.args)
    logger.info(f"Received /teamstats request for: {team_name_query}")

    found_teams = await find_team(team_name_query)

    if not found_teams:
        await update.message.reply_text(f"Sorry, I couldn't find a team matching '{team_name_query}'.")
        return
    if len(found_teams) > 1:
        await update.message.reply_text(f"Found multiple teams for '{team_name_query}'. Please be more specific.\n"
                                        f"Matches: {[t['full_name'] for t in found_teams[:5]]}")
        return

    team_info = found_teams[0]
    team_id = team_info['id']
    team_full_name = team_info['full_name']
    current_season = get_season_string()

    try:
        dash_stats = leaguedashteamstats.LeagueDashTeamStats(
            season=current_season,
            per_mode_detailed='PerGame'
        )
        stats_df = dash_stats.get_data_frames()[0]
        team_stats_row = stats_df[stats_df['TEAM_ID'] == team_id]

        if team_stats_row.empty:
            await update.message.reply_text(f"Could not find {current_season} season stats for the {team_full_name}.")
            return

        stats = team_stats_row.iloc[0]
        wins = stats['W']
        losses = stats['L']
        win_pct = stats['W_PCT'] * 100 if pd.notna(stats['W_PCT']) else 0
        pts = stats['PTS']
        reb = stats['REB']
        ast = stats['AST']
        stl = stats['STL']
        blk = stats['BLK']
        fg_pct = stats['FG_PCT'] * 100 if pd.notna(stats['FG_PCT']) else 0
        fg3_pct = stats['FG3_PCT'] * 100 if pd.notna(stats['FG3_PCT']) else 0
        ft_pct = stats['FT_PCT'] * 100 if pd.notna(stats['FT_PCT']) else 0
        off_rating = stats.get('OFF_RATING', 'N/A')
        def_rating = stats.get('DEF_RATING', 'N/A')
        net_rating = stats.get('NET_RATING', 'N/A')

        message = (
            f"📊 **{team_full_name} ({current_season} Season Stats)**\n\n"
            f"Record: {wins}-{losses} ({win_pct:.1f}%)\n"
            f"Points: {pts:.1f} PPG\n"
            f"Rebounds: {reb:.1f} RPG\n"
            f"Assists: {ast:.1f} APG\n"
            f"Steals: {stl:.1f} SPG\n"
            f"Blocks: {blk:.1f} BPG\n\n"
            f"FG%: {fg_pct:.1f}%\n"
            f"3P%: {fg3_pct:.1f}%\n"
            f"FT%: {ft_pct:.1f}%\n\n"
            f"Offensive Rating: {off_rating}\n"
            f"Defensive Rating: {def_rating}\n"
            f"Net Rating: {net_rating}"
        )
        await update.message.reply_text(message, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Error fetching stats for team ID {team_id} ({team_full_name}): {e}")
        await update.message.reply_text(f"Sorry, an error occurred while fetching stats for {team_full_name}.")


async def next_game_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Finds the next scheduled game for a team (Limited Accuracy)."""
    if not context.args:
        await update.message.reply_text("Please provide a team name.\nUsage: `/nextgame [team_name]`")
        return

    team_name_query = " ".join(context.args)
    logger.info(f"Received /nextgame request for: {team_name_query}")

    found_teams = await find_team(team_name_query)

    if not found_teams:
        await update.message.reply_text(f"Sorry, I couldn't find a team matching '{team_name_query}'.")
        return
    if len(found_teams) > 1:
        await update.message.reply_text(f"Found multiple teams for '{team_name_query}'. Please be more specific.\n"
                                        f"Matches: {[t['full_name'] for t in found_teams[:5]]}")
        return

    team_info = found_teams[0]
    team_id = team_info['id']
    team_full_name = team_info['full_name']
    
    logger.info(f"Looking for next game for team: {team_full_name} (ID: {team_id})")

    try:
        # Use the current season for more accurate results
        current_season = get_season_string()
        logger.info(f"Using season: {current_season}")
        
        # Try to get games for the current season
        try:
            finder = leaguegamefinder.LeagueGameFinder(
                team_id_nullable=team_id,
                season_nullable=current_season
            )
            games_df = finder.get_data_frames()[0]
        except Exception as api_err:
            logger.error(f"Error with primary API call: {api_err}")
            # Fallback to a simpler query without season
            logger.info("Trying fallback API call without season parameter")
            finder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id)
            games_df = finder.get_data_frames()[0]
        
        logger.info(f"Found {len(games_df)} games for {team_full_name}")

        if games_df.empty:
            await update.message.reply_text(f"Could not find any game data for the {team_full_name}.")
            return

        # Log the first few rows to understand the data structure
        logger.info(f"Game data sample: {games_df.head(2).to_dict()}")
        
        # Check if GAME_DATE column exists
        if 'GAME_DATE' not in games_df.columns:
            logger.error(f"GAME_DATE column not found in game data. Columns: {games_df.columns.tolist()}")
            await update.message.reply_text("Error: Game date information not available in the API response.")
            return
            
        # Try to parse dates with more robust error handling
        try:
            # First try standard format
            games_df['GAME_DATETIME'] = pd.to_datetime(games_df['GAME_DATE'], errors='coerce')
            
            # Check if we have any valid dates
            if games_df['GAME_DATETIME'].isna().all():
                logger.warning("All dates are NaN after first parsing attempt, trying alternative format")
                # Try alternative format
                games_df['GAME_DATETIME'] = pd.to_datetime(games_df['GAME_DATE'], format='%b %d, %Y', errors='coerce')
                
                # If still all NaN, try another format
                if games_df['GAME_DATETIME'].isna().all():
                    logger.warning("All dates are NaN after second parsing attempt, trying another format")
                    games_df['GAME_DATETIME'] = pd.to_datetime(games_df['GAME_DATE'], format='%Y-%m-%d', errors='coerce')
        except Exception as date_err:
            logger.error(f"Error parsing game dates: {date_err}")
            await update.message.reply_text("Error parsing game dates. Cannot determine next game.")
            return

        # Remove rows with NaN dates
        games_df = games_df.dropna(subset=['GAME_DATETIME'])
        
        if games_df.empty:
            logger.error("No valid dates found after parsing")
            await update.message.reply_text("Error: No valid game dates found in the API response.")
            return
            
        # Get current date in the NBA timezone
        now = datetime.datetime.now(NBA_TZ)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Filter for future games and sort by date
        future_games = games_df[games_df['GAME_DATETIME'] >= today].sort_values(by='GAME_DATETIME')
        
        logger.info(f"Found {len(future_games)} future games for {team_full_name}")

        if future_games.empty:
            await update.message.reply_text(f"Couldn't find any upcoming games for the {team_full_name} in the available data. Schedule might be outdated.")
            return

        next_game = future_games.iloc[0]
        game_date = next_game['GAME_DATETIME']
        
        # Format the date in a user-friendly way
        game_date_str = game_date.strftime('%a, %b %d, %Y')
        
        # Get matchup information
        matchup = next_game.get('MATCHUP', 'Unknown Opponent')
        
        # Try to get game time if available
        game_time = ""
        if 'GAME_TIME' in next_game and pd.notna(next_game['GAME_TIME']):
            game_time = f"\n🕒 Time: {next_game['GAME_TIME']}"
        
        # Try to get location if available
        location = ""
        if 'HOME_TEAM_ID' in next_game and pd.notna(next_game['HOME_TEAM_ID']):
            is_home = next_game['HOME_TEAM_ID'] == team_id
            location = "Home" if is_home else "Away"

        message = (
            f"⏭️ **Next Game for {team_full_name}**\n\n"
            f"📅 Date: {game_date_str}{game_time}\n"
            f"📍 Location: {location}\n"
            f"🆚 Matchup: {matchup}\n\n"
            f"_(Note: Schedule data might have delays. Game time isn't always available here.)_"
        )
        await update.message.reply_text(message, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Error fetching next game for team ID {team_id} ({team_full_name}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        await update.message.reply_text(f"Sorry, an error occurred while fetching the next game for {team_full_name}.")


async def standings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fetches and displays current league standings."""
    logger.info("Received /standings request")
    current_season = get_season_string()

    try:
        standings_data = leaguestandingsv3.LeagueStandingsV3(season=current_season)
        standings_df = standings_data.get_data_frames()[0]

        if standings_df.empty:
            await update.message.reply_text(f"Could not retrieve league standings for the {current_season} season.")
            return

        for rank_col in ['PlayoffRank', 'ConferenceRank', 'DivisionRank']:
            if rank_col in standings_df.columns:
                standings_df[rank_col] = pd.to_numeric(standings_df[rank_col], errors='coerce')

        if 'ConferenceRank' in standings_df.columns:
            standings_df = standings_df.sort_values(by=['Conference', 'ConferenceRank'], ascending=[True, True])
        elif 'PlayoffRank' in standings_df.columns:
            standings_df = standings_df.sort_values(by=['Conference', 'PlayoffRank'], ascending=[True, True])
        else:
            standings_df = standings_df.sort_values(by=['Conference', 'WinPCT'], ascending=[True, False])

        east_standings = []
        west_standings = []

        for index, team in standings_df.iterrows():
            rank = team.get('ConferenceRank', index + 1)
            try:
                rank = int(rank)
            except (ValueError, TypeError):
                rank = '?'

            team_name = team.get('TeamCity', '') + " " + team.get('TeamName', 'Unknown Team')
            record = team.get('Record', 'N/A')
            win_pct = team.get('WinPCT', 0) * 100 if pd.notna(team.get('WinPCT')) else 0
            streak = team.get('CurrentStreak', 'N/A')

            line = f"{rank}. {team_name} ({record}) - {win_pct:.1f}% ({streak})"

            if team['Conference'] == 'East':
                east_standings.append(line)
            elif team['Conference'] == 'West':
                west_standings.append(line)

        message = f"🏆 **NBA Standings ({current_season})**\n\n"
        message += "**Eastern Conference**\n" + "\n".join(east_standings) + "\n\n"
        message += "**Western Conference**\n" + "\n".join(west_standings)

        if len(message) > 4096:
            midpoint = message.find("**Western Conference**")
            if midpoint != -1:
                await update.message.reply_text(message[:midpoint], parse_mode='Markdown')
                await update.message.reply_text(message[midpoint:], parse_mode='Markdown')
            else:
                await update.message.reply_text(message[:4090] + "\n...", parse_mode='Markdown')
        else:
            await update.message.reply_text(message, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Error fetching league standings: {e}")
        await update.message.reply_text("Sorry, an error occurred while fetching league standings.")
        import traceback
        traceback.print_exc()


# --- Following Feature Commands (Using Database) ---

async def follow_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Adds a player to the user's follow list in the database."""
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Please provide a player name to follow.\nUsage: `/follow [player_name]`")
        return

    player_name_query = " ".join(context.args)

    found_players = await find_player(player_name_query)
    if not found_players:
        await update.message.reply_text(f"Hmm, I couldn't find an active player matching '{player_name_query}'. Please check the name.")
        return
    if len(found_players) > 1:
        await update.message.reply_text(f"Found multiple players for '{player_name_query}'. Following the first match: {found_players[0]['full_name']}. Be more specific next time if this is wrong.")

    # Get player info from the first match
    player_info = found_players[0]
    player_to_follow_id = player_info['id']
    player_to_follow_name = player_info['full_name'] # Use the canonical name

    # Add to database using the new function signature
    was_added = add_follow(chat_id, player_to_follow_id, player_to_follow_name)

    if was_added:
        logger.info(f"User {chat_id} started following {player_to_follow_name} (ID: {player_to_follow_id})")
        await update.message.reply_text(f"✅ You are now following {player_to_follow_name}!")
    else:
        # Check if it failed because they already follow, or a DB error occurred
        # We query the DB to be sure why it failed (already exists is most likely)
        current_follows = get_followed_players(chat_id)
        already_following = any(p_id == player_to_follow_id for p_id, name in current_follows)
        if already_following:
            await update.message.reply_text(f"You are already following {player_to_follow_name}.")
        else:
            # If not already following, it must have been a DB error logged by add_follow
            await update.message.reply_text(f"An error occurred while trying to follow {player_to_follow_name}. Please try again later.")


async def unfollow_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Removes a player from the user's follow list in the database."""
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Please provide a player name to unfollow.\nUsage: `/unfollow [player_name]`")
        return

    player_name_query = " ".join(context.args)

    # Use the modified remove_follow (still works by name)
    was_removed = remove_follow(chat_id, player_name_query)

    if was_removed:
        logger.info(f"User {chat_id} unfollowed {player_name_query}")
        await update.message.reply_text(f"❌ You are no longer following {player_name_query}.")
    else:
        current_follows = get_followed_players(chat_id)
        if not current_follows:
            await update.message.reply_text("You weren't following any players.")
        else:
            # Check if the name actually exists in their follows for a better message
            followed_names = [name for p_id, name in current_follows]
            if player_name_query not in followed_names:
                await update.message.reply_text(f"You weren't following anyone named '{player_name_query}'.\n"
                                                f"Use `/following` to see exact names.")
            else:
                # If name was correct but remove failed, likely DB error
                await update.message.reply_text(f"An error occurred trying to unfollow '{player_name_query}'.")


async def following_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lists the players the user is currently following from the database."""
    chat_id = update.effective_chat.id

    # Get list from database (returns tuples of id, name)
    followed_list_tuples = get_followed_players(chat_id)

    if not followed_list_tuples:
        await update.message.reply_text("You aren't following any players yet. Use `/follow [player_name]` to start!")
        return

    # Extract just the names for display
    followed_names = [name for p_id, name in followed_list_tuples]

    message = "⭐ **You are following:**\n\n" + "\n".join(f"- {name}" for name in followed_names)
    await update.message.reply_text(message, parse_mode='Markdown')

    # List is already sorted by the DB query
    message = "⭐ **You are following:**\n\n" + "\n".join(f"- {name}" for name in followed_names)
    await update.message.reply_text(message, parse_mode='Markdown')


# --- Bot Setup and Run ---

async def post_init(application: Application):
    """Sets the bot commands visible in Telegram clients."""
    commands = [
        BotCommand("start", "Start the bot and see help"),
        BotCommand("help", "Show help message"),
        BotCommand("playerstats", "Get player season stats (e.g., /playerstats LeBron James)"),
        BotCommand("lastgame", "Get player's last game stats (e.g., /lastgame Curry)"),
        BotCommand("teamroster", "Get team roster (e.g., /teamroster Lakers)"),
        BotCommand("teamstats", "Get team season stats (e.g., /teamstats Celtics)"),
        BotCommand("nextgame", "Get team's next game (e.g., /nextgame Knicks)"),
        BotCommand("standings", "Get league standings"),
        BotCommand("follow", "Follow a player (e.g., /follow Doncic)"),
        BotCommand("unfollow", "Unfollow a player (e.g., /unfollow Doncic)"),
        BotCommand("following", "List players you follow"),
    ]
    await application.bot.set_my_commands(commands)
    logger.info("Bot commands set successfully.")


if __name__ == "__main__":
    if not TELEGRAM_TOKEN:
        logger.error("FATAL: TELEGRAM_BOT_TOKEN environment variable not set.")
        exit(1)

        # --- Initialize Database ---
    try:
        init_db() # This will now create/update both tables
    except sqlite3.Error:
        logger.error("FATAL: Could not initialize the database. Exiting.")
        exit(1)

    logger.info("Starting bot...")

    builder = Application.builder().token(TELEGRAM_TOKEN)
    builder.post_init(post_init)
    application = builder.build()

    # --- Job Queue Setup ---
    job_queue = application.job_queue

    # Check if job queue is available
    if job_queue is not None:
        job_upcoming = job_queue.run_daily(
            check_upcoming_games,
            time=datetime.time(hour=12, minute=0, second=0),
            name="check_upcoming_games_daily"
        )
        job_finished = job_queue.run_daily(
            check_finished_games,
            time=datetime.time(hour=16, minute=0, second=0),
            name="check_finished_games_daily"
        )
        logger.info("Scheduled daily jobs: Upcoming check at 12:00, Finished check at 16:00 (server time/UTC)")
    else:
        logger.warning("Job queue is not available. Scheduled jobs will not run. Install python-telegram-bot[job-queue] to enable this feature.")

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("playerstats", player_stats_command))
    application.add_handler(CommandHandler("lastgame", last_game_command))
    application.add_handler(CommandHandler("teamroster", team_roster_command))
    application.add_handler(CommandHandler("teamstats", team_stats_command))
    application.add_handler(CommandHandler("nextgame", next_game_command))
    application.add_handler(CommandHandler("standings", standings_command))
    application.add_handler(CommandHandler("follow", follow_command))
    application.add_handler(CommandHandler("unfollow", unfollow_command))
    application.add_handler(CommandHandler("following", following_command))


    logger.info("Running application.run_polling()...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)