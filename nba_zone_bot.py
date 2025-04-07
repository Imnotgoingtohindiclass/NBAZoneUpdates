import logging
import os
import datetime
import sqlite3
from dotenv import load_dotenv
import pandas as pd

from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes, ApplicationBuilder

# --- NBA API Imports ---
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import (
    playercareerstats,
    playergamelog,
    commonteamroster,
    teamyearbyyearstats,
    leaguegamefinder,
    leaguestandingsv3,
    leaguedashteamstats
)
from nba_api.stats.library.parameters import SeasonAll

# --- Configuration ---
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CURRENT_SEASON = '2024-25'
DB_FILE = 'nba_bot_data.db'

# --- Logging Setup ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Database Functions ---

def init_db():
    """Initializes the database and creates the follows table if it doesn't exist."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            # Create table with case-insensitive collation for player names
            # Use PRIMARY KEY constraint to prevent duplicate entries per user
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_player_follows (
                    chat_id INTEGER NOT NULL,
                    player_full_name TEXT NOT NULL COLLATE NOCASE,
                    PRIMARY KEY (chat_id, player_full_name)
                )
            ''')
            conn.commit()
            logger.info(f"Database {DB_FILE} initialized successfully.")
    except sqlite3.Error as e:
        logger.error(f"Database initialization error: {e}")
        raise # Reraise the error to potentially stop the bot if DB is critical

def add_follow(chat_id: int, player_name: str) -> bool:
    """Adds a player to a user's follow list in the database. Returns True if added, False if already exists."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            # INSERT OR IGNORE will do nothing if the (chat_id, player_name) pair already exists
            cursor.execute('''
                INSERT OR IGNORE INTO user_player_follows (chat_id, player_full_name)
                VALUES (?, ?)
            ''', (chat_id, player_name))
            conn.commit()
            # cursor.rowcount will be 1 if a row was inserted, 0 if it was ignored (already exists)
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"Error adding follow for chat {chat_id}, player {player_name}: {e}")
        return False # Indicate failure

def remove_follow(chat_id: int, player_name: str) -> bool:
    """Removes a player from a user's follow list. Returns True if removed, False otherwise."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM user_player_follows
                WHERE chat_id = ? AND player_full_name = ?
            ''', (chat_id, player_name))
            conn.commit()
            # cursor.rowcount will be 1 if a row was deleted, 0 if no matching row was found
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"Error removing follow for chat {chat_id}, player {player_name}: {e}")
        return False # Indicate failure

def get_followed_players(chat_id: int) -> list[str]:
    """Retrieves the list of players a user follows."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT player_full_name FROM user_player_follows
                WHERE chat_id = ?
                ORDER BY player_full_name COLLATE NOCASE ASC
            ''', (chat_id,))
            # fetchall() returns a list of tuples, e.g., [('LeBron James',), ('Stephen Curry',)]
            # We extract the first element from each tuple.
            followed = [row[0] for row in cursor.fetchall()]
            return followed
    except sqlite3.Error as e:
        logger.error(f"Error fetching followed players for chat {chat_id}: {e}")
        return [] # Return empty list on error

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

    try:
        finder = leaguegamefinder.LeagueGameFinder(team_id_nullable=team_id)
        games_df = finder.get_data_frames()[0]

        if games_df.empty:
            await update.message.reply_text(f"Could not find any game data for the {team_full_name}.")
            return

        try:
            games_df['GAME_DATETIME'] = pd.to_datetime(games_df['GAME_DATE'], errors='coerce')
        except ValueError:
            try:
                games_df['GAME_DATETIME'] = pd.to_datetime(games_df['GAME_DATE'], format='%b %d, %Y', errors='coerce')
            except Exception as date_err:
                logger.error(f"Could not parse GAME_DATE format: {date_err}")
                await update.message.reply_text("Error parsing game dates. Cannot determine next game.")
                return

        games_df = games_df.dropna(subset=['GAME_DATETIME'])
        today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        future_games = games_df[games_df['GAME_DATETIME'] >= today].sort_values(by='GAME_DATETIME')

        if future_games.empty:
            await update.message.reply_text(f"Couldn't find any upcoming games for the {team_full_name} in the available data. Schedule might be outdated.")
            return

        next_game = future_games.iloc[0]
        game_date_str = next_game['GAME_DATETIME'].strftime('%a, %b %d, %Y')
        matchup = next_game['MATCHUP']

        message = (
            f"⏭️ **Next Game for {team_full_name}**\n\n"
            f"📅 Date: {game_date_str}\n"
            f"🆚 Matchup: {matchup}\n\n"
            f"_(Note: Schedule data might have delays. Game time isn't always available here.)_"
        )
        await update.message.reply_text(message, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Error fetching next game for team ID {team_id} ({team_full_name}): {e}")
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

    # Validate player exists using NBA API
    found_players = await find_player(player_name_query)
    if not found_players:
        await update.message.reply_text(f"Hmm, I couldn't find an active player matching '{player_name_query}'. Please check the name.")
        return
    if len(found_players) > 1:
        await update.message.reply_text(f"Found multiple players for '{player_name_query}'. Following the first match: {found_players[0]['full_name']}. Be more specific next time if this is wrong.")

    # Use the canonical name from the search results
    player_to_follow = found_players[0]['full_name']

    # Add to database
    was_added = add_follow(chat_id, player_to_follow)

    if was_added:
        logger.info(f"User {chat_id} started following {player_to_follow}")
        await update.message.reply_text(f"✅ You are now following {player_to_follow}!")
    else:
        # Check if it failed because they already follow, or a DB error occurred
        # (add_follow logs DB errors, so here we assume it means they already follow)
        await update.message.reply_text(f"You are already following {player_to_follow}.")


async def unfollow_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Removes a player from the user's follow list in the database."""
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("Please provide a player name to unfollow.\nUsage: `/unfollow [player_name]`")
        return

    # Important: Use the exact player name format the user provides,
    # as our DB query uses COLLATE NOCASE for matching.
    player_name_query = " ".join(context.args)

    # Remove from database
    was_removed = remove_follow(chat_id, player_name_query)

    if was_removed:
        logger.info(f"User {chat_id} unfollowed {player_name_query}")
        await update.message.reply_text(f"❌ You are no longer following {player_name_query}.")
    else:
        # remove_follow returns False if player wasn't found for that user or DB error
        # Check if they follow anything at all first for a better message
        current_follows = get_followed_players(chat_id)
        if not current_follows:
            await update.message.reply_text("You weren't following any players.")
        else:
            await update.message.reply_text(f"You weren't following anyone matching '{player_name_query}'.\n"
                                            f"Use `/following` to see who you follow.")


async def following_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lists the players the user is currently following from the database."""
    chat_id = update.effective_chat.id

    # Get list from database
    followed_list = get_followed_players(chat_id)

    if not followed_list:
        await update.message.reply_text("You aren't following any players yet. Use `/follow [player_name]` to start!")
        return

    # List is already sorted by the DB query
    message = "⭐ **You are following:**\n\n" + "\n".join(f"- {name}" for name in followed_list)
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
        init_db()
    except sqlite3.Error:
        logger.error("FATAL: Could not initialize the database. Exiting.")
        exit(1)
    # -------------------------

    logger.info("Starting bot...")

    builder = Application.builder().token(TELEGRAM_TOKEN)
    builder.post_init(post_init)
    application = builder.build()

    # Register command handlers
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