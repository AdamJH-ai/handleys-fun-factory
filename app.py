import os
import json
import random
from datetime import datetime
from flask import Flask, render_template, request # Removed unused 'session' import
from flask_socketio import SocketIO, emit, join_room, leave_room
import eventlet # Recommended for stability

# --- Basic Setup ---
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'a_super_secret_key_change_me!')
# Use eventlet if installed: pip install eventlet
socketio = SocketIO(app, async_mode='eventlet', cors_allowed_origins="*")

# === GAME CONFIG ===
GAME_ROUNDS_TOTAL = 6
AVAILABLE_ROUND_TYPES = ['guess_the_age', 'guess_the_year', 'who_didnt_do_it', 'order_up', 'quick_pairs', 'true_or_false']
#AVAILABLE_ROUND_TYPES = ['true_or_false']
MAX_PLAYERS = 8
gta_target_turns = 10
gty_target_turns = 10
wddi_target_turns = 10
ou_target_turns = 10
qp_target_turns = 10
tf_target_turns = 10
QP_NUM_PAIRS_PER_QUESTION = 3

# === ROUND DETAILS ===
ROUND_RULES = {
    'guess_the_age': "Guess the celebrity's current age! Score based on difference (lowest wins).",
    'guess_the_year': "Guess the year the event happened! Score based on difference (lowest wins).",
    'who_didnt_do_it': "Identify the option that doesn't fit the question! Score based on correct answers (most wins).",
    'order_up': "Arrange the items in the correct order! Score 1 point for each perfect order.",
    'quick_pairs': "Quickly match all pairs! Fastest correct gets 2 points, others correct get 1.",
    'true_or_false': "Decide if the statement is true or false. You get one point for each correct answer.",
}
ROUND_DISPLAY_NAMES = {
    'guess_the_age': "Guess The Age",
    'guess_the_year': "Guess The Year",
    'who_didnt_do_it': "Who Didn't Do It?",
    'order_up': "Order Up!",
    'quick_pairs': "Quick Pairs",
    'true_or_false': "True or False",
}
ROUND_JINGLES = {
    'guess_the_age': 'gta_jingle.mp3',
    'guess_the_year': 'gty_jingle.mp3',
    'who_didnt_do_it': 'wddi_jingle.mp3',
    'order_up': 'ou_jingle.mp3',
    'quick_pairs': 'qp_jingle.mp3',
    'true_or_false': 'tf_jingle.mp3'
    # 'team_round': 'team_jingle.mp3' # For the future
}
ROUND_INTRO_DELAY = 8 # Seconds

# === GAME STATE ===
game_state = "waiting"
current_game_round_num = 0
selected_rounds_for_game = []
overall_game_scores = {} # {sid: game_points}
players = {} # {sid: {'name':'N', 'round_score':0, 'gta_guess':None, 'gty_guess':None}} # Removed WDDI fields
main_screen_sid = None

# === GUESS THE AGE STATE ===
gta_celebrities = []; gta_shuffled_celebrities_this_round = []
gta_current_celebrity = None; gta_current_celebrity_index = -1; gta_actual_turns_this_round = 0

# === GUESS THE YEAR STATE ===
gty_questions = []; gty_shuffled_questions_this_round = []
gty_current_question = None; gty_current_question_index = -1; gty_actual_turns_this_round = 0

# === WHO DIDN'T DO IT STATE ===
wddi_questions = [] # Holds all loaded questions
wddi_shuffled_questions_this_round = [] # Holds the 10 questions selected for the current round
wddi_current_question = None # Holds the question data for the current turn
wddi_current_question_index = -1 # Index for the current turn within the round
wddi_actual_turns_this_round = 0 # Number of turns/questions in this specific round (usually 10)
wddi_current_shuffled_options = [] # Holds the shuffled options for the *current* turn
# Note: We will add wddi_current_guess to the players dict later

# === ORDER UP STATE ===  # 
ou_questions = []  # Holds all loaded "Order Up!" questions
ou_shuffled_questions_this_round = [] # Holds questions selected for the current round
ou_current_question_data = None # Holds the full data for the current turn's question (incl. correct order)
ou_current_question_index = -1 # Index for the current turn/question
ou_actual_turns_this_round = 0 # Number of turns for this round
# ou_current_shuffled_items_for_players = [] # We'll generate this on the fly in next_order_up_turn

# === QUICK PAIRS STATE ===  # 
qp_questions = []  # Holds all loaded "Quick Pairs" questions
qp_shuffled_questions_this_round = [] # Holds questions selected for the current round
qp_current_question_data = None # Holds the full data for the current turn's question (incl. correct pairs)
qp_current_question_index = -1
qp_actual_turns_this_round = 0
# qp_player_completion_times = {} # Will store {sid: completion_time_ms} for players who get all pairs correct

# === TRUE OR FALSE STATE ===  # 
tf_questions = []
tf_shuffled_questions_this_round = []
tf_current_question = None
tf_current_question_index = -1
tf_actual_turns_this_round = 0

# === ROOMS ===
MAIN_ROOM = 'main_room'; PLAYERS_ROOM = 'players_room'

# === DATA LOADING ===
def load_guess_the_age_data(filename="celebrities.json"):
    global gta_celebrities; gta_celebrities = []
    try:
        with open(filename, 'r', encoding='utf-8') as f: data = json.load(f)
        print(f"[GTAData] Loaded {len(data)} potential from {filename}")
        today = datetime.today(); processed = []
        for c in data:
            try:
                if not all(k in c for k in ('name','dob','image_url','description')): continue
                dob = datetime.strptime(c['dob'], '%Y-%m-%d')
                c['age'] = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
                processed.append(c)
            except Exception as e: print(f"[GTAData] Skip Err({e}): {c.get('name')}")
        gta_celebrities = processed; print(f"[GTAData] OK: {len(gta_celebrities)}")
    except Exception as e: print(f"[GTAData] Load Fail: {e}")

def load_guess_the_year_data(filename="guess_the_year_questions.json"):
    global gty_questions; gty_questions = []
    try:
        with open(filename, 'r', encoding='utf-8') as f: data = json.load(f)
        print(f"[GTYData] Loaded {len(data)} potential from {filename}")
        gty_questions = [q for q in data if q.get('question') and q.get('year') and isinstance(q.get('year'), int) and q.get('image_url')] # Ensure image_url exists
        print(f"[GTYData] OK: {len(gty_questions)} valid.")
        if not gty_questions: print("[GTYData] WARN: No valid questions.")
    except Exception as e: print(f"[GTYData] Load Fail: {e}")

def load_who_didnt_do_it_data(filename="who_didnt_do_it_questions.json"):
    """Loads questions for the 'Who Didn't Do It?' round."""
    global wddi_questions; wddi_questions = []
    filepath = filename
    # filepath = filename # If file is in the same directory as app.py
    try:
        # Adjust 'filepath' if your json isn't in static/
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"[WDDI_Data] Loaded {len(data)} potential questions from {filename}")
        processed_questions = []
        for q in data:
            # Validate required fields and structure
            if not all(k in q for k in ('question', 'options', 'correct_answer')) or \
               not isinstance(q['options'], list) or len(q['options']) != 6 or \
               not q['correct_answer'] or q['correct_answer'] not in q['options']:
                print(f"[WDDI_Data] Skipping invalid entry: {q.get('question', 'N/A')}")
                continue
            # Add optional image_url if present, otherwise set to None
            q['image_url'] = q.get('image_url', None)
            processed_questions.append(q)

        wddi_questions = processed_questions
        print(f"[WDDI_Data] OK: {len(wddi_questions)} valid questions loaded.")
        if not wddi_questions:
            print("[WDDI_Data] WARN: No valid questions loaded for 'Who Didn't Do It?'.")
    except FileNotFoundError:
         print(f"[WDDI_Data] ERROR: File not found at {filepath}")
    except json.JSONDecodeError as e:
         print(f"[WDDI_Data] ERROR: Failed to parse JSON in {filename}: {e}")
    except Exception as e:
        print(f"[WDDI_Data] Load Fail: An unexpected error occurred: {e}")

def load_order_up_data(filename="order_up_questions.json"): # <-- Add this new function
    """Loads questions for the 'Order Up!' round."""
    global ou_questions; ou_questions = []
    filepath = filename # Assumes file is in the root directory with app.py
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"[OU_Data] Loaded {len(data)} potential questions from {filename}")
        processed_questions = []
        for q_idx, q in enumerate(data):
            # Validate required fields and structure
            if not all(k in q for k in ('question', 'items_in_correct_order')) or \
               not isinstance(q['items_in_correct_order'], list) or \
               not q['question'].strip(): # Ensure question is not empty
                print(f"[OU_Data] Skipping invalid entry (Index {q_idx}): Missing fields or empty question.")
                continue
            if not q['items_in_correct_order']: # Ensure items list is not empty
                print(f"[OU_Data] Skipping invalid entry (Index {q_idx}, Q: '{q.get('question', '')[:30]}...'): Empty items list.")
                continue
            # Optional: Validate number of items if we want to enforce it (e.g., exactly 4)
            # For now, let's be flexible but recommend 4.
            # if len(q['items_in_correct_order']) != 4:
            #     print(f"[OU_Data] WARN: Question (Index {q_idx}) does not have exactly 4 items. Q: {q.get('question')}")

            processed_questions.append(q)

        ou_questions = processed_questions
        print(f"[OU_Data] OK: {len(ou_questions)} valid 'Order Up!' questions loaded.")
        if not ou_questions:
            print("[OU_Data] WARN: No valid questions loaded for 'Order Up!'.")
    except FileNotFoundError:
        print(f"[OU_Data] ERROR: File not found at {filepath}")
    except json.JSONDecodeError as e:
        print(f"[OU_Data] ERROR: Failed to parse JSON in {filename}: {e}")
    except Exception as e:
        print(f"[OU_Data] Load Fail: An unexpected error occurred: {e}")

def load_quick_pairs_data(filename="quick_pairs_questions.json"): # <-- Add this new function
    """Loads questions for the 'Quick Pairs' round."""
    global qp_questions
    qp_questions = []
    filepath = filename
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"[QP_Data] Loaded {len(data)} potential questions from {filename}")
        processed_questions = []
        for q_idx, q_data in enumerate(data):
            if not all(k in q_data for k in ('category_prompt', 'pairs')) or \
               not q_data['category_prompt'].strip() or \
               not isinstance(q_data['pairs'], list) or \
               len(q_data['pairs']) != QP_NUM_PAIRS_PER_QUESTION: # Check for exact number of pairs
                print(f"[QP_Data] Skipping invalid entry (Index {q_idx}): Incorrect format or pair count. Expected {QP_NUM_PAIRS_PER_QUESTION} pairs.")
                continue

            valid_pairs = True
            for pair_idx, pair in enumerate(q_data['pairs']):
                if not isinstance(pair, list) or len(pair) != 2 or \
                   not str(pair[0]).strip() or not str(pair[1]).strip():
                    print(f"[QP_Data] Skipping question (Index {q_idx}, Prompt: '{q_data.get('category_prompt', '')[:30]}...') due to invalid pair at pair index {pair_idx}: {pair}")
                    valid_pairs = False
                    break
            if not valid_pairs:
                continue

            processed_questions.append(q_data)

        qp_questions = processed_questions
        print(f"[QP_Data] OK: {len(qp_questions)} valid 'Quick Pairs' questions loaded.")
        if not qp_questions:
            print("[QP_Data] WARN: No valid questions loaded for 'Quick Pairs'.")
    except FileNotFoundError:
        print(f"[QP_Data] ERROR: File not found at {filepath}")
    except json.JSONDecodeError as e:
        print(f"[QP_Data] ERROR: Failed to parse JSON in {filename}: {e}")
    except Exception as e:
        print(f"[QP_Data] Load Fail for Quick Pairs: An unexpected error occurred: {e}")

def load_true_or_false_data(filename="true_or_false_questions.json"):
    global tf_questions
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"[TF_Data] Loaded {len(data)} potential questions from {filename}")
        # Basic validation
        tf_questions = [q for q in data if 'statement' in q and 'correct_answer' in q and isinstance(q['correct_answer'], bool)]
        print(f"[TF_Data] OK: {len(tf_questions)} valid 'True or False' questions loaded.")
    except Exception as e:
        print(f"[TF_Data] Load Fail: {e}")

# === HELPERS ===
def update_main_screen_html(target_selector, template_name, context):
    """Renders a template fragment and sends it to the main screen."""
    if main_screen_sid:
        try:
            html_content = render_template(template_name, **context)
            socketio.emit('update_html', {
                'target_selector': target_selector,
                'html': html_content
            }, room=main_screen_sid)
        except Exception as e:
            print(f"ERROR rendering template {template_name}: {e}")

def emit_player_list_update():
    """Sends updated player list HTML."""
    player_names = [p['name'] for p in players.values()]
    update_main_screen_html('#player-list', '_player_list.html', {'player_names': player_names})

def emit_game_state_update():
    """Sends non-HTML game state info (scores, round nums, etc.)."""
    if main_screen_sid:
        scores_list = sorted([{'name': p['name'], 'game_score': overall_game_scores.get(sid, 0)}
                              for sid, p in players.items()], key=lambda x: x['game_score'], reverse=True)
        payload = {
            'game_state': game_state,
            'current_game_round_num': current_game_round_num,
            'game_rounds_total': GAME_ROUNDS_TOTAL,
            'overall_scores': scores_list,
            'current_round_type': selected_rounds_for_game[current_game_round_num-1] if 0 < current_game_round_num <= len(selected_rounds_for_game) else None
        }
        socketio.emit('game_state_update', payload, room=main_screen_sid)

# <<< Corrected Stableford Scoring Logic >>>
def award_game_points(sorted_player_sids_by_round_score):
    global overall_game_scores; num_players = len(sorted_player_sids_by_round_score);
    if num_players == 0: return {}
    print(f"Awarding points for {num_players} players...")
    points_by_rank = {}
    for rank in range(1, num_players + 1): points_by_rank[rank] = (num_players + 1) if rank == 1 and num_players > 1 else (2 if rank == 1 and num_players == 1 else num_players - rank + 1)
    print(f"  Points structure (Rank: Points): {points_by_rank}"); points_awarded_this_round = {}; i = 0
    while i < num_players:
        current_sid = sorted_player_sids_by_round_score[i]; current_player_info = players.get(current_sid)
        if not current_player_info: i += 1; continue
        current_round_score = current_player_info.get('round_score', None); tied_sids = [current_sid]; j = i + 1
        while j < num_players:
            next_sid=sorted_player_sids_by_round_score[j]; next_player_info=players.get(next_sid)
            if not next_player_info or next_player_info.get('round_score', None) != current_round_score: break
            tied_sids.append(next_sid); j += 1
        num_tied = len(tied_sids); rank_start = i + 1; rank_end = i + num_tied
        if num_tied == 1: points = points_by_rank.get(rank_start, 0)
        else: sum_points = sum(points_by_rank.get(r, 0) for r in range(rank_start, rank_end + 1)); points = round(sum_points / num_tied, 1); print(f"  Tie ranks {rank_start}-{rank_end} avg: {points}")
        for tied_sid in tied_sids:
            if tied_sid in overall_game_scores: points_awarded_this_round[tied_sid] = points; overall_game_scores[tied_sid] = overall_game_scores.get(tied_sid, 0) + points; print(f"  - {players.get(tied_sid,{}).get('name','?')} gets {points} pts. Total: {overall_game_scores[tied_sid]}")
        i += num_tied
    return points_awarded_this_round

# Helpers for checking guesses
def check_all_guesses_received_gta(): return all(p.get('gta_current_guess') is not None for p in players.values()) if players else True
def check_all_guesses_received_gty(): return all(p.get('gty_current_guess') is not None for p in players.values()) if players else True
# REMOVED WDDI check

# === ROUTES ===
@app.route('/')
def index(): return render_template('index.html')
@app.route('/main')
def main_screen_route(): return render_template('main_screen.html')

# === SOCKET.IO HANDLERS ===
@socketio.on('connect')
def handle_connect(): print(f"Client connected: {request.sid}")

@socketio.on('disconnect')
def handle_disconnect():
    player_sid = request.sid; global main_screen_sid
    if player_sid == main_screen_sid: print("Main Screen disconnected."); main_screen_sid = None; leave_room(MAIN_ROOM, player_sid)
    elif player_sid in players:
        player_name = players.pop(player_sid)['name']; overall_game_scores.pop(player_sid, None)
        print(f"Player {player_name} disconnected."); leave_room(PLAYERS_ROOM, player_sid);
        emit_player_list_update(); emit_game_state_update()
        # Check results conditions for GTA and GTY only
        if game_state == 'guess_age_ongoing' and check_all_guesses_received_gta(): process_guess_age_turn_results()
        elif game_state == 'guess_the_year_ongoing' and check_all_guesses_received_gty(): process_guess_the_year_turn_results()
        elif game_state == 'who_didnt_do_it_ongoing' and check_all_guesses_received_wddi(): process_who_didnt_do_it_turn_results()
        elif game_state == 'order_up_ongoing' and check_all_submissions_received_ou(): process_order_up_turn_results()   
        elif game_state == 'quick_pairs_ongoing' and check_all_submissions_received_qp(): process_quick_pairs_turn_results()
    else: print(f"Unregistered client disconnected: {player_sid}")

@socketio.on('register_main_screen')
def handle_register_main_screen():
    global main_screen_sid; player_sid = request.sid
    if main_screen_sid and main_screen_sid != player_sid: print(f"WARN: New main screen {player_sid}.")
    leave_room(PLAYERS_ROOM, player_sid); join_room(MAIN_ROOM, player_sid); main_screen_sid = player_sid
    print(f"Main Screen registered: {main_screen_sid}")
    emit_player_list_update(); emit_game_state_update()

@socketio.on('register_player')
def handle_register_player(data):
    player_sid = request.sid; player_name = str(data.get('name', f'P_{player_sid[:4]}')).strip()[:15] or f'P_{player_sid[:4]}'
    if player_sid == main_screen_sid: return
    if len(players) >= MAX_PLAYERS and player_sid not in players: emit('message', {'data': 'Game full.'}, room=player_sid); return
    if player_sid not in players:
        join_room(PLAYERS_ROOM, player_sid)
        # Initialize only GTA and GTY fields
        players[player_sid] = {'name': player_name, 'round_score': 0, 'gta_current_guess': None, 'gty_current_guess': None, 'wddi_current_guess': None, 'ou_current_submission': None, 'qp_current_submission': None, 'qp_submission_time_ms': float('inf')} # Store submission time, default to infinity
        overall_game_scores[player_sid] = 0; print(f"Player registered: {player_name} ({player_sid[:4]})")
        emit('message', {'data': f'Welcome {player_name}!'}, room=player_sid)
    else: players[player_sid]['name'] = player_name; emit('message', {'data': f'Rejoined as {player_name}.'}, room=player_sid)
    emit_player_list_update(); emit_game_state_update()
    # Simplified join mid-game handling
    if game_state.endswith('_ongoing'): emit('message', {'data': 'Game in progress, wait...'}, room=player_sid)
    elif game_state.endswith('_results'): emit('results_on_main_screen', room=player_sid)
    elif game_state == 'overall_game_over': emit('overall_game_over_player', room=player_sid)
    elif game_state == 'waiting': emit('message', {'data': f'Welcome {player_name}! Waiting.'}, room=player_sid)

# === OVERALL GAME FLOW ===
@socketio.on('start_game_request')
def handle_start_overall_game_request():
    global game_state, current_game_round_num, selected_rounds_for_game, overall_game_scores
    if request.sid != main_screen_sid or game_state != "waiting": return
    if not players or not AVAILABLE_ROUND_TYPES: print("ERR: Cannot start."); return
    print(f"Overall Game start request."); game_state = "game_ongoing"; current_game_round_num = 0
    overall_game_scores = {sid: 0 for sid in players};
    num_avail = len(AVAILABLE_ROUND_TYPES)
    if num_avail >= GAME_ROUNDS_TOTAL: selected_rounds_for_game = random.sample(AVAILABLE_ROUND_TYPES, GAME_ROUNDS_TOTAL)
    else: selected_rounds_for_game = (AVAILABLE_ROUND_TYPES * (GAME_ROUNDS_TOTAL // num_avail + 1))[:GAME_ROUNDS_TOTAL]; random.shuffle(selected_rounds_for_game)
    print(f"Selected rounds: {selected_rounds_for_game}"); emit_game_state_update(); socketio.sleep(1); start_next_game_round()

# <<< Dispatcher only calls GTA and GTY >>>
def start_next_game_round():
    global current_game_round_num, game_state
    current_game_round_num += 1; print(f"\n===== Prep Game Rnd {current_game_round_num}/{GAME_ROUNDS_TOTAL} =====")
    if current_game_round_num > GAME_ROUNDS_TOTAL: end_overall_game(); return

    round_type_key = selected_rounds_for_game[current_game_round_num - 1]
    round_type_name = ROUND_DISPLAY_NAMES.get(round_type_key, round_type_key)
    round_rules = ROUND_RULES.get(round_type_key, "No rules.")

    print(f"Round Type: {round_type_name}"); game_state = "round_intro"; emit_game_state_update()

    # --- Play Jingle ---
    jingle_file = ROUND_JINGLES.get(round_type_key)
    if jingle_file and main_screen_sid:
        print(f"   Emitting jingle: {jingle_file} for round {round_type_key}")
        socketio.emit('play_round_jingle', {'jingle_file': jingle_file}, room=main_screen_sid)
    elif not jingle_file:
        print(f"   WARN: No jingle found for round type: {round_type_key}")
    # --------------------

    intro_context = {'game_round_num': current_game_round_num, 'game_rounds_total': GAME_ROUNDS_TOTAL, 'round_type_name': round_type_name, 'round_rules': round_rules }
    update_main_screen_html('#results-area', '_round_intro.html', intro_context)
    print(f"Show intro {ROUND_INTRO_DELAY}s..."); socketio.sleep(ROUND_INTRO_DELAY)
    if game_state != "round_intro": print("WARN: State changed during intro."); return
    # Dispatch to GTA or GTY only
    if round_type_key == 'guess_the_age': setup_guess_age_round()
    elif round_type_key == 'guess_the_year': setup_guess_the_year_round()
    elif round_type_key == 'who_didnt_do_it': setup_who_didnt_do_it_round()
    elif round_type_key == 'order_up': setup_order_up_round()
    elif round_type_key == 'quick_pairs': setup_quick_pairs_round() 
    elif round_type_key == 'true_or_false': setup_true_or_false_round()
    else: print(f"ERR: Unknown/Removed type {round_type_key}. Skip."); socketio.sleep(1); start_next_game_round()

def end_overall_game():
    global game_state; print("\n***** OVERALL GAME OVER *****"); game_state = "overall_game_over"
    emit_game_state_update(); final_scores = []
    sorted_players = sorted(overall_game_scores.items(), key=lambda item: item[1], reverse=True)
    final_scores = [{'rank': r+1, 'name': players.get(sid, {}).get('name', '?'), 'game_score': score} for r, (sid, score) in enumerate(sorted_players)]
    print("Final Scores:", final_scores)
    update_main_screen_html('#overall-game-over-area', '_overall_game_over.html', {'scores': final_scores})
    socketio.emit('overall_game_over_player', room=PLAYERS_ROOM); print("Sent overall game over notices.")
    socketio.sleep(15); game_state = "waiting"; print("State reset to waiting.")
    if main_screen_sid: emit_game_state_update(); socketio.emit('ready_for_new_game', room=main_screen_sid)

# === GUESS THE AGE LOGIC ===
# (setup_guess_age_round, next_guess_age_turn, handle_submit_gta_guess, process_guess_age_turn_results, end_guess_age_round - Reverted to the state before WDDI was added, includes debug logs)
def setup_guess_age_round():
    global game_state, gta_shuffled_celebrities_this_round, gta_current_celebrity_index, gta_actual_turns_this_round, gta_celebrities, gta_target_turns
    print("--- Setup GTA Round ---"); game_state = "guess_age_ongoing"
    if not gta_celebrities: print("ERR: No celebs for GTA."); start_next_game_round(); return
    for sid in players: players[sid]['round_score'] = 0; players[sid]['gta_current_guess'] = None
    gta_actual_turns_this_round = min(gta_target_turns, len(gta_celebrities)); gta_shuffled_celebrities_this_round = random.sample(gta_celebrities, gta_actual_turns_this_round)
    gta_current_celebrity_index = -1; print(f"GTA Round: {gta_actual_turns_this_round} turns."); emit_game_state_update(); socketio.sleep(0.5); next_guess_age_turn()
def next_guess_age_turn():
    global game_state, gta_current_celebrity, gta_current_celebrity_index, gta_actual_turns_this_round
    gta_current_celebrity_index += 1;
    if gta_current_celebrity_index >= gta_actual_turns_this_round: end_guess_age_round(); return
    game_state = "guess_age_ongoing"; gta_current_celebrity = gta_shuffled_celebrities_this_round[gta_current_celebrity_index]
    for sid in players: players[sid]['gta_current_guess'] = None
    print(f"\n-- GTA Turn {gta_current_celebrity_index + 1}/{gta_actual_turns_this_round} -- Celeb: {gta_current_celebrity['name']}")
    context = {'turn': gta_current_celebrity_index + 1, 'total_turns': gta_actual_turns_this_round,'celebrity': gta_current_celebrity, 'players_status': [{'name': p['name']} for p in players.values()]}
    update_main_screen_html('#round-content-area', '_gta_turn_display.html', context); player_payload = { 'celebrity_name': gta_current_celebrity['name'] }; socketio.emit('gta_player_prompt', player_payload, room=PLAYERS_ROOM)
@socketio.on('submit_gta_guess')
def handle_submit_gta_guess(data):
    player_sid = request.sid;
    if player_sid in players and game_state == "guess_age_ongoing":
        try:
            guess = int(data.get('guess')); assert 0 <= guess <= 120
            if players[player_sid].get('gta_current_guess') is None:
                players[player_sid]['gta_current_guess'] = guess; player_name = players[player_sid]['name']; print(f"GTA Guess {guess} from {player_name}({player_sid[:4]})")
                remaining = sum(1 for p in players.values() if p.get('gta_current_guess') is None); emit('gta_wait_for_guesses', {'waiting_on': remaining}, room=player_sid)
                safe_name_id = player_name.replace('[^a-zA-Z0-9-_]', '_'); socketio.emit('player_submitted_update', {'name': player_name}, room=main_screen_sid)
                if check_all_guesses_received_gta(): print("All GTA guesses received."); socketio.sleep(0.5); process_guess_age_turn_results()
            else: emit('message', {'data': 'Already guessed.'}, room=player_sid)
        except Exception as e: emit('message', {'data': 'Invalid guess (0-120).'}, room=player_sid); print(f"Invalid GTA guess: {e}")
def process_guess_age_turn_results():
    global game_state; print(f"DEBUG: Entered process_guess_age_turn_results. State: {game_state}");
    if game_state != "guess_age_ongoing": print("DEBUG: Exiting GTA process early."); return; print("--- Processing GTA Turn Results ---");
    results_context = { 'results': [], 'actual_age': None }; print("DEBUG: Defined results_context GTA.");
    if gta_current_celebrity:
        actual_age = gta_current_celebrity['age']; results_context['actual_age'] = actual_age; print(f"Actual Age: {actual_age}"); round_results_list = []
        active_players_copy = list(players.items()); print(f"DEBUG: GTA Processing for {len(active_players_copy)} players.");
        for sid, p_info in active_players_copy:
            print(f"DEBUG: GTA Loop - Player {p_info.get('name', '?')}"); guess = p_info.get('gta_current_guess'); print(f"DEBUG:   -> Guess: {guess}"); score_diff = abs(actual_age - guess) if guess is not None else None; print(f"DEBUG:   -> Diff: {score_diff}");
            if 'round_score' not in p_info: p_info['round_score'] = 0
            if score_diff is not None: p_info['round_score'] = p_info.get('round_score', 0) + score_diff; print(f"DEBUG:   -> New Rnd Score: {p_info['round_score']}")
            result_entry = {'name': p_info.get('name', '?'),'guess': guess if guess is not None else 'N/A','diff': score_diff if score_diff is not None else '-','round_score': p_info.get('round_score', 0)}; round_results_list.append(result_entry); print(f"DEBUG:   -> Appended: {result_entry}")
        print(f"DEBUG: GTA finished loop. List size: {len(round_results_list)}"); results_context['results'] = sorted(round_results_list, key=lambda r: r['diff'] if isinstance(r['diff'], int) else float('inf')); print(f"DEBUG: Final GTA results context: {results_context}")
        update_main_screen_html('#results-area', '_gta_turn_results.html', results_context)
    else: print("Error: process_gta_turn_results - no celeb.")
    socketio.sleep(5);
    if game_state == "guess_age_ongoing": print("DEBUG: Proceeding next GTA turn."); next_guess_age_turn()
    else: print(f"DEBUG: State changed GTA sleep ({game_state}).")
def end_guess_age_round():
    global game_state;
    game_state = "guess_age_results"; # Set state FIRST
    print("\n--- Ending GTA Round ---");
    # Don't emit game state update yet, scores haven't been awarded

    # 1. Determine rankings (lower round_score is better rank)
    active_players = [(sid, p.get('round_score', float('inf'))) for sid, p in players.items()]
    # Sort by score (ascending), then name alphabetically for stable tie ranks
    sorted_by_round = sorted(active_players, key=lambda item: (item[1], players.get(item[0],{}).get('name','')))
    sorted_sids = [item[0] for item in sorted_by_round]

    # <<< Log BEFORE awarding points >>>
    print(f"DEBUG: Overall scores BEFORE award_game_points: {overall_game_scores}")

    # 2. Award game points (This modifies the global overall_game_scores)
    points_awarded = award_game_points(sorted_sids)

    # <<< Log AFTER awarding points >>>
    print(f"DEBUG: Overall scores AFTER award_game_points: {overall_game_scores}")
    print(f"DEBUG: Points awarded this round: {points_awarded}")

    # <<< Emit game state update AFTER scores are calculated >>>
    # This updates the status bar with the latest scores
    emit_game_state_update()

    # 3. Prepare payload using the *updated* global scores for the summary screen
    rankings_this_round = []
    for rank, sid in enumerate(sorted_sids):
        if sid in players: # Check player still exists
            rankings_this_round.append({
                'rank': rank + 1,
                'name': players[sid]['name'],
                'round_score': players[sid]['round_score'],
                'points_awarded': points_awarded.get(sid, 0) # Include points awarded
            })

    # Generate the overall scores list *now* based on the updated global dictionary
    current_overall_scores_list = [{'name': p['name'], 'game_score': overall_game_scores.get(sid, 0)}
                                   for sid, p in players.items()]
    # Sort this list for display consistency (e.g., by score descending)
    current_overall_scores_list.sort(key=lambda x: x['game_score'], reverse=True)


    summary_context = {
        'round_type': ROUND_DISPLAY_NAMES.get('guess_the_age', 'Guess The Age'),
        'rankings': rankings_this_round,
        'overall_scores': current_overall_scores_list # Use the freshly generated list
    }
    print(f"DEBUG: Summary Context being sent: {summary_context}") # Log context

    # Send HTML for summary screen (now includes correct overall scores)
    update_main_screen_html('#results-area', '_round_summary.html', summary_context)
    print("Sent 'round_over_summary' HTML.")

    # 4. Pause and move to the next *game* round
    round_summary_display_time = 12;
    print(f"Waiting {round_summary_display_time}s before next game round...")
    socketio.sleep(round_summary_display_time)

    # Check state hasn't changed during sleep before proceeding
    if game_state == "guess_age_results":
        start_next_game_round()
    else:
        print(f"WARN: Game state changed during round summary sleep ({game_state}). Not proceeding automatically.")

# === GUESS THE YEAR LOGIC ===
# (setup_guess_the_year_round, next_guess_the_year_turn, handle_submit_gty_guess, process_guess_the_year_turn_results, end_guess_the_year_round - Reverted to state before WDDI, includes debug logs)
def setup_guess_the_year_round():
    global game_state, gty_shuffled_questions_this_round, gty_current_question_index, gty_actual_turns_this_round, gty_questions, gty_target_turns
    print("--- Setup GTY Round ---"); game_state = "guess_the_year_ongoing";
    if not gty_questions: print("ERR: No questions GTY."); start_next_game_round(); return
    for sid in players: players[sid]['round_score'] = 0; players[sid]['gty_current_guess'] = None
    gty_actual_turns_this_round = min(gty_target_turns, len(gty_questions)); gty_shuffled_questions_this_round = random.sample(gty_questions, gty_actual_turns_this_round)
    gty_current_question_index = -1; print(f"GTY Round: {gty_actual_turns_this_round} turns."); emit_game_state_update(); socketio.sleep(0.5); next_guess_the_year_turn()
def next_guess_the_year_turn():
    global game_state, gty_current_question, gty_current_question_index, gty_actual_turns_this_round
    gty_current_question_index += 1;
    if gty_current_question_index >= gty_actual_turns_this_round: end_guess_the_year_round(); return
    game_state = "guess_the_year_ongoing"; gty_current_question = gty_shuffled_questions_this_round[gty_current_question_index]
    for sid in players: players[sid]['gty_current_guess'] = None
    print(f"\n-- GTY Turn {gty_current_question_index + 1}/{gty_actual_turns_this_round} -- Q: {gty_current_question['question']}"); print(f"   (Ans: {gty_current_question['year']})")
    context = {'turn': gty_current_question_index + 1, 'total_turns': gty_actual_turns_this_round,'question_data': gty_current_question,'players_status': [{'name': p['name']} for p in players.values()]}
    update_main_screen_html('#round-content-area', '_gty_turn_display.html', context); player_payload = { 'question': gty_current_question['question'] }; socketio.emit('gty_player_prompt', player_payload, room=PLAYERS_ROOM)
@socketio.on('submit_gty_guess')
def handle_submit_gty_guess(data):
    player_sid = request.sid;
    if player_sid in players and game_state == "guess_the_year_ongoing":
        try:
            guess = int(data.get('guess')); assert -10000 <= guess <= datetime.now().year + 100
            if players[player_sid].get('gty_current_guess') is None:
                players[player_sid]['gty_current_guess'] = guess; player_name = players[player_sid]['name']; print(f"GTY Guess {guess} from {player_name}({player_sid[:4]})")
                remaining = sum(1 for p in players.values() if p.get('gty_current_guess') is None); emit('gty_wait_for_guesses', {'waiting_on': remaining}, room=player_sid)
                safe_name_id = player_name.replace('[^a-zA-Z0-9-_]', '_'); socketio.emit('player_submitted_update', {'name': player_name}, room=main_screen_sid)
                if check_all_guesses_received_gty(): print("All GTY guesses received."); socketio.sleep(0.5); process_guess_the_year_turn_results()
            else: emit('message', {'data': 'Already guessed.'}, room=player_sid)
        except Exception as e: emit('message', {'data': 'Invalid year.'}, room=player_sid); print(f"Invalid GTY guess: {e}")
def process_guess_the_year_turn_results():
    global game_state; print(f"DEBUG: Entered process_gty_turn_results. State: {game_state}");
    if game_state != "guess_the_year_ongoing": print("DEBUG: Exiting GTY process early."); return; print("--- Processing GTY Turn Results ---");
    results_context = { 'results': [], 'correct_year': None, 'question_text': '' }; print("DEBUG: Defined results_context GTY.");
    if gty_current_question:
        correct_year = gty_current_question['year']; results_context['correct_year'] = correct_year; results_context['question_text'] = gty_current_question['question']; print(f"Actual Year: {correct_year}"); round_results_list = []
        active_players_copy = list(players.items()); print(f"DEBUG: GTY Processing for {len(active_players_copy)} players.");
        for sid, p_info in active_players_copy:
            print(f"DEBUG: GTY Loop - Player {p_info.get('name', '?')}"); guess = p_info.get('gty_current_guess'); print(f"DEBUG:   -> Guess: {guess}"); score_diff = abs(correct_year - guess) if guess is not None else None; print(f"DEBUG:   -> Diff: {score_diff}");
            if 'round_score' not in p_info: p_info['round_score'] = 0
            if score_diff is not None: p_info['round_score'] = p_info.get('round_score', 0) + score_diff; print(f"DEBUG:   -> New Rnd Score: {p_info['round_score']}")
            result_entry = {'name': p_info.get('name', '?'),'guess': guess if guess is not None else 'N/A','diff': score_diff if score_diff is not None else '-','round_score': p_info.get('round_score', 0)}; round_results_list.append(result_entry); print(f"DEBUG:   -> Appended GTY: {result_entry}")
        print(f"DEBUG: GTY finished loop. List size: {len(round_results_list)}"); results_context['results'] = sorted(round_results_list, key=lambda r: r['diff'] if isinstance(r['diff'], int) else float('inf')); print(f"DEBUG: Final GTY results context: {results_context}")
        update_main_screen_html('#results-area', '_gty_turn_results.html', results_context)
    else: print("Error: process_gty_turn_results - no question.")
    socketio.sleep(5);
    if game_state == "guess_the_year_ongoing": print("DEBUG: Proceeding next GTY turn."); next_guess_the_year_turn()
    else: print(f"DEBUG: State changed GTY sleep ({game_state}).")
def end_guess_the_year_round():
    global game_state;
    game_state = "guess_the_year_results"; # Set state FIRST
    print("\n--- Ending GTY Round ---");
    # Don't emit game state update yet, scores haven't been awarded

    # 1. Determine rankings (lower round_score is better rank)
    active_players = [(sid, p.get('round_score', float('inf'))) for sid, p in players.items()]
    # Sort by score (ascending), then name alphabetically for stable tie ranks
    sorted_by_round = sorted(active_players, key=lambda item: (item[1], players.get(item[0],{}).get('name','')))
    sorted_sids = [item[0] for item in sorted_by_round]

    # <<< Log BEFORE awarding points >>>
    print(f"DEBUG: Overall scores BEFORE award_game_points: {overall_game_scores}")

    # 2. Award game points (Modifies global overall_game_scores)
    points_awarded = award_game_points(sorted_sids)

    # <<< Log AFTER awarding points >>>
    print(f"DEBUG: Overall scores AFTER award_game_points: {overall_game_scores}")
    print(f"DEBUG: Points awarded this round: {points_awarded}")

    # <<< Emit game state update AFTER scores are calculated >>>
    emit_game_state_update() # Updates status bar

    # 3. Prepare payload using updated scores
    rankings_this_round = []
    for rank, sid in enumerate(sorted_sids):
        if sid in players:
            rankings_this_round.append({
                'rank': rank + 1,
                'name': players[sid]['name'],
                'round_score': players[sid]['round_score'],
                'points_awarded': points_awarded.get(sid, 0)
            })

    current_overall_scores_list = [{'name': p['name'], 'game_score': overall_game_scores.get(sid, 0)}
                                   for sid, p in players.items()]
    current_overall_scores_list.sort(key=lambda x: x['game_score'], reverse=True) # Sort display list

    summary_context = {
        'round_type': ROUND_DISPLAY_NAMES.get('guess_the_year', 'Guess The Year'),
        'rankings': rankings_this_round,
        'overall_scores': current_overall_scores_list
    }
    print(f"DEBUG: Summary Context being sent: {summary_context}")

    # Send HTML for summary screen
    update_main_screen_html('#results-area', '_round_summary.html', summary_context)
    print("Sent 'round_over_summary' HTML.")

    # 4. Pause and move to next game round
    round_summary_display_time = 12;
    print(f"Waiting {round_summary_display_time}s before next game round...")
    socketio.sleep(round_summary_display_time)

    if game_state == "guess_the_year_results":
        start_next_game_round()
    else:
         print(f"WARN: Game state changed during round summary sleep ({game_state}). Not proceeding.")

# === WHO DIDN'T DO IT LOGIC ===
# Helper to check if all players have submitted their guess for the current WDDI turn
def check_all_guesses_received_wddi():
    """Checks if all connected players have submitted a WDDI guess for the current turn."""
    if not players:
        return True # No players, so technically all received
    return all(p.get('wddi_current_guess') is not None for p in players.values())

def setup_who_didnt_do_it_round():
    """Sets up the state for a 'Who Didn't Do It?' round."""
    global game_state, wddi_shuffled_questions_this_round, wddi_current_question_index
    global wddi_actual_turns_this_round, wddi_questions, wddi_target_turns
    print("--- Setup WDDI Round ---")
    game_state = "who_didnt_do_it_ongoing" # Set the specific game state

    if not wddi_questions:
        print("ERROR: No questions loaded for 'Who Didn't Do It?'. Skipping round.")
        start_next_game_round() # Skip to next round if no data
        return

    # Reset round scores and guesses for all players
    for sid in players:
        players[sid]['round_score'] = 0 # Reset round score (higher is better here)
        players[sid]['wddi_current_guess'] = None # Reset guess for the round start

    # Select questions for the round
    wddi_actual_turns_this_round = min(wddi_target_turns, len(wddi_questions))
    wddi_shuffled_questions_this_round = random.sample(wddi_questions, wddi_actual_turns_this_round)
    wddi_current_question_index = -1 # Start before the first turn

    print(f"WDDI Round starting with {wddi_actual_turns_this_round} questions.")
    emit_game_state_update() # Update main screen status bar
    socketio.sleep(0.5) # Short pause before first turn
    next_who_didnt_do_it_turn() # Start the first turn

def next_who_didnt_do_it_turn():
    """Advances to the next turn/question in the WDDI round."""
    global game_state, wddi_current_question, wddi_current_question_index
    global wddi_shuffled_questions_this_round, wddi_actual_turns_this_round
    global wddi_current_shuffled_options # Store shuffled options for validation

    wddi_current_question_index += 1

    # Check if round is over
    if wddi_current_question_index >= wddi_actual_turns_this_round:
        end_who_didnt_do_it_round() # All questions asked, end the round
        return

    game_state = "who_didnt_do_it_ongoing" # Ensure state is correct
    wddi_current_question = wddi_shuffled_questions_this_round[wddi_current_question_index]

    # Clear previous guesses for all players
    for sid in players:
        players[sid]['wddi_current_guess'] = None

    # --- Prepare options and shuffle them ---
    original_options = list(wddi_current_question['options']) # Make a copy
    wddi_current_shuffled_options = original_options # Assign before shuffling for context
    random.shuffle(wddi_current_shuffled_options) # Shuffle the list in place

    print(f"\n-- WDDI Turn {wddi_current_question_index + 1}/{wddi_actual_turns_this_round} --")
    print(f"   Q: {wddi_current_question['question']}")
    # print(f"   DEBUG: Shuffled Options: {wddi_current_shuffled_options}") # Optional debug log
    print(f"   Correct Answer: {wddi_current_question['correct_answer']}") # For server log/debug

    # --- Send data to Main Screen ---
    # Context for the main screen display template (_wddi_turn_display.html)
    main_screen_context = {
        'turn': wddi_current_question_index + 1,
        'total_turns': wddi_actual_turns_this_round,
        'question_text': wddi_current_question['question'],
        'image_url': wddi_current_question.get('image_url'), # Include image_url if present
        'shuffled_options': wddi_current_shuffled_options, # Send shuffled options
        'players_status': [{'name': p['name']} for p in players.values()] # For showing who hasn't guessed
    }
    # Assuming you have/will create '_wddi_turn_display.html' in templates/
    update_main_screen_html('#round-content-area', '_wddi_turn_display.html', main_screen_context)

    # --- Send data to Player Controllers ---
    # Payload for the player devices (index.html's JS)
    player_payload = {
        'question': wddi_current_question['question'],
        'shuffled_options': wddi_current_shuffled_options # Send the same shuffled list
        # image_url could be sent here too if players need to see it on their device
    }
    # We need a unique event name for this round's player prompt
    socketio.emit('wddi_player_prompt', player_payload, room=PLAYERS_ROOM)
    print("   Sent question and shuffled options to players.")

@socketio.on('submit_wddi_guess')
def handle_submit_wddi_guess(data):
    """Handles a player submitting their guess for the current WDDI turn."""
    player_sid = request.sid
    if player_sid not in players or game_state != "who_didnt_do_it_ongoing":
        print(f"WARN: Guess rejected from {player_sid[:4]}. State: {game_state}")
        return # Ignore if player not registered or not in the correct game state

    guess_text = data.get('guess_text') # Expecting the text of the chosen option

    # Basic validation: is the guess one of the options sent?
    if not guess_text or guess_text not in wddi_current_shuffled_options:
         emit('message', {'data': 'Invalid selection.'}, room=player_sid)
         print(f"WDDI Invalid guess received: '{guess_text}' from {players[player_sid]['name']}")
         return

    if players[player_sid].get('wddi_current_guess') is None:
        # Store the submitted text as the guess
        players[player_sid]['wddi_current_guess'] = guess_text
        player_name = players[player_sid]['name']
        print(f"WDDI Guess '{guess_text}' received from {player_name}({player_sid[:4]})")

        # Notify player their guess was received (optional)
        # emit('wddi_wait_for_others', room=player_sid) # Or similar feedback

        # Update main screen to show player has guessed (optional, good UI)
        safe_name_id = player_name.replace('[^a-zA-Z0-9-_]', '_') # Create a CSS-safe ID
        socketio.emit('player_submitted_update', {'name': player_name}, room=main_screen_sid)

        # Check if all players have now guessed
        if check_all_guesses_received_wddi():
            print("   All WDDI guesses received.")
            socketio.sleep(0.5) # Brief pause before showing results
            process_who_didnt_do_it_turn_results()
    else:
        # Player already submitted a guess for this turn
        emit('message', {'data': 'You already guessed for this question.'}, room=player_sid)
        print(f"WDDI Duplicate guess attempt from {players[player_sid]['name']}")

def process_who_didnt_do_it_turn_results():
    """Processes guesses, calculates scores, and sends results for a WDDI turn."""
    global game_state
    print(f"--- Processing WDDI Turn Results (Index: {wddi_current_question_index}) ---")
    if game_state != "who_didnt_do_it_ongoing" or not wddi_current_question:
        print(f"WARN: Skipping WDDI results processing. State: {game_state}, Question: {wddi_current_question is not None}")
        return # Avoid processing if state changed or question missing

    game_state = "who_didnt_do_it_results_display" # Temp state while showing results

    correct_answer_text = wddi_current_question['correct_answer']
    turn_results_list = []

    print(f"   Correct Answer was: '{correct_answer_text}'")

    active_players_copy = list(players.items()) # Copy to avoid issues if player disconnects during loop
    for sid, p_info in active_players_copy:
        guess = p_info.get('wddi_current_guess')
        was_correct = (guess == correct_answer_text)
        turn_score = 1 if was_correct else 0

        # Update the player's *round score* (cumulative correct answers)
        if 'round_score' not in p_info: p_info['round_score'] = 0 # Ensure exists
        p_info['round_score'] += turn_score
        print(f"   - Player: {p_info['name']}, Guess: '{guess}', Correct: {was_correct}, New Round Score: {p_info['round_score']}")

        turn_results_list.append({
            'name': p_info['name'],
            'guess': guess if guess is not None else 'N/A',
            'is_correct': was_correct,
            'round_score': p_info['round_score'] # Cumulative round score
        })

    # Sort results (e.g., by correctness then name) for display
    turn_results_list.sort(key=lambda x: (-int(x['is_correct']), x['name']))

    # --- Send results to Main Screen ---
    # Context for the results template (_wddi_turn_results.html - Needs creating)
    results_context = {
        'question_text': wddi_current_question['question'],
        'image_url': wddi_current_question.get('image_url'),
        'shuffled_options': wddi_current_shuffled_options, # Show options again
        'correct_answer': correct_answer_text,
        'results': turn_results_list, # List of player results for the turn
        'turn': wddi_current_question_index + 1,
        'total_turns': wddi_actual_turns_this_round
    }
    # NOTE: You will need to create a '_wddi_turn_results.html' template file!
    update_main_screen_html('#results-area', '_wddi_turn_results.html', results_context)
    print(f"   Sent WDDI turn results to main screen.")
    # Send simple notification to players that results are shown
    socketio.emit('results_on_main_screen', room=PLAYERS_ROOM)

    # Pause to show results
    turn_results_display_time = 7 # Seconds to show turn results
    socketio.sleep(turn_results_display_time)

    # Check state hasn't changed during sleep before proceeding
    if game_state == "who_didnt_do_it_results_display":
        print(f"   Proceeding to next WDDI turn/end of round.")
        next_who_didnt_do_it_turn() # Move to the next turn
    else:
        print(f"WARN: Game state changed during WDDI results sleep ({game_state}). Not proceeding automatically.")


def end_who_didnt_do_it_round():
    """Finalizes the WDDI round, awards game points, and transitions."""
    global game_state
    game_state = "who_didnt_do_it_results" # Final round results state
    print("\n--- Ending WDDI Round ---")

    # 1. Determine rankings based on round_score (higher is better for WDDI)
    active_players = [(sid, p.get('round_score', 0)) for sid, p in players.items()]
    # Sort by score (descending), then name alphabetically for stable tie ranks
    sorted_by_round = sorted(active_players, key=lambda item: (-item[1], players.get(item[0],{}).get('name','')))
    sorted_sids = [item[0] for item in sorted_by_round]
    print(f"   WDDI Round Ranks (SID, Score): {sorted_by_round}")

    # 2. Award Stableford game points (using existing helper)
    print(f"   Overall scores BEFORE award_game_points: {overall_game_scores}")
    # Pass the SIDs sorted by rank (higher score = better rank for WDDI)
    points_awarded = award_game_points(sorted_sids)
    print(f"   Overall scores AFTER award_game_points: {overall_game_scores}")
    print(f"   Points awarded this round: {points_awarded}")

    # 3. Emit game state update AFTER scores are calculated (updates status bar)
    emit_game_state_update()

    # 4. Prepare payload for the round summary screen using updated scores
    rankings_this_round = []
    for rank, sid in enumerate(sorted_sids):
        if sid in players: # Check player still exists
            rankings_this_round.append({
                'rank': rank + 1,
                'name': players[sid]['name'],
                'round_score': players[sid]['round_score'], # Show number correct
                'points_awarded': points_awarded.get(sid, 0)
            })

    # Get the latest overall scores for the summary display
    current_overall_scores_list = [{'name': p['name'], 'game_score': overall_game_scores.get(sid, 0)}
                                   for sid, p in players.items()]
    current_overall_scores_list.sort(key=lambda x: x['game_score'], reverse=True) # Sort for display

    summary_context = {
        'round_type': ROUND_DISPLAY_NAMES.get('who_didnt_do_it', 'Who Didn\'t Do It?'),
        'rankings': rankings_this_round, # WDDI round results (higher score = better)
        'overall_scores': current_overall_scores_list # Updated overall game scores
    }
    print(f"   Summary Context being sent: {summary_context}")

    # Use the existing _round_summary.html template
    update_main_screen_html('#results-area', '_round_summary.html', summary_context)
    print("   Sent 'round_over_summary' HTML.")

    # 5. Pause and move to the next game round
    round_summary_display_time = 12 # Seconds
    print(f"   Waiting {round_summary_display_time}s before next game round...")
    socketio.sleep(round_summary_display_time)

    # Check state hasn't changed during sleep before proceeding
    if game_state == "who_didnt_do_it_results":
        start_next_game_round() # Trigger the overall game flow handler
    else:
        print(f"WARN: Game state changed during WDDI summary sleep ({game_state}). Not proceeding automatically.")

# === ORDER UP LOGIC ===

def check_all_submissions_received_ou():
    """Checks if all connected players have submitted their 'Order Up!' list for the current turn."""
    if not players:
        return True # No players, so technically all received
    # Check if the 'ou_current_submission' is not None for all players
    return all(p.get('ou_current_submission') is not None for p in players.values())

def setup_order_up_round():
    """Sets up the state for an 'Order Up!' round."""
    global game_state, ou_shuffled_questions_this_round, ou_current_question_index
    global ou_actual_turns_this_round, ou_questions, ou_target_turns
    print("--- Setup Order Up! Round ---")
    game_state = "order_up_ongoing"

    if not ou_questions:
        print("ERROR: No questions loaded for 'Order Up!'. Skipping round.")
        start_next_game_round()
        return

    # Reset round scores and submissions for all players
    for sid in players:
        players[sid]['round_score'] = 0 # Reset round score (for 'all or nothing' correct orders)
        players[sid]['ou_current_submission'] = None # Reset submission for the round start

    # Select questions for the round
    ou_actual_turns_this_round = min(ou_target_turns, len(ou_questions))
    if ou_actual_turns_this_round == 0 and ou_questions: # If target_turns is 0 but questions exist
        ou_actual_turns_this_round = len(ou_questions) # Use all available if target is 0
    elif ou_actual_turns_this_round == 0:
        print("ERROR: No turns to play for 'Order Up!' (0 questions or 0 target_turns). Skipping round.")
        start_next_game_round()
        return

    ou_shuffled_questions_this_round = random.sample(ou_questions, ou_actual_turns_this_round)
    ou_current_question_index = -1 # Start before the first turn

    print(f"Order Up! Round starting with {ou_actual_turns_this_round} questions.")
    emit_game_state_update()
    socketio.sleep(0.5) # Short pause before first turn
    next_order_up_turn()

def next_order_up_turn():
    """Advances to the next turn/question in the 'Order Up!' round."""
    global game_state, ou_current_question_data, ou_current_question_index
    global ou_shuffled_questions_this_round, ou_actual_turns_this_round

    ou_current_question_index += 1

    if ou_current_question_index >= ou_actual_turns_this_round:
        end_order_up_round() # All questions asked, end the round
        return

    game_state = "order_up_ongoing"
    current_question_full_data = ou_shuffled_questions_this_round[ou_current_question_index]
    ou_current_question_data = current_question_full_data # Store full data including correct order

    # Clear previous submissions for all players for the new turn
    for sid in players:
        players[sid]['ou_current_submission'] = None

    # Prepare the list of items to be shuffled and sent to players
    items_to_order_original = list(ou_current_question_data['items_in_correct_order']) # Make a copy
    items_shuffled_for_players = list(items_to_order_original) # Another copy for shuffling
    random.shuffle(items_shuffled_for_players)

    print(f"\n-- Order Up! Turn {ou_current_question_index + 1}/{ou_actual_turns_this_round} --")
    print(f"   Q: {ou_current_question_data['question']}")
    print(f"   Correct Order (Server): {ou_current_question_data['items_in_correct_order']}") # For server log/debug
    print(f"   Shuffled for Players: {items_shuffled_for_players}") # Optional debug

    # --- Send data to Main Screen ---
    # Context for a new main screen display template (e.g., _ou_turn_display.html)
    main_screen_context = {
        'turn': ou_current_question_index + 1,
        'total_turns': ou_actual_turns_this_round,
        'question_text': ou_current_question_data['question'],
        'items_to_display': items_shuffled_for_players, # Main screen could show the shuffled items too, or just the question
        'players_status': [{'name': p['name']} for p in players.values()]
    }
    # NOTE: You will need to create an '_ou_turn_display.html' template
    update_main_screen_html('#round-content-area', '_ou_turn_display.html', main_screen_context)

    # --- Send data to Player Controllers ---
    player_payload = {
        'question': ou_current_question_data['question'],
        'items_to_order': items_shuffled_for_players # Send the shuffled list for players to order
    }
    print(f"DEBUG SERVER: Emitting 'ou_player_prompt' to PLAYERS_ROOM. Payload: {player_payload}")
    socketio.emit('ou_player_prompt', player_payload, room=PLAYERS_ROOM)
    print("   Sent 'Order Up!' question and items to players.")


@socketio.on('submit_ou_list') # Changed event name from 'submit_ou_guess'
def handle_submit_ou_list(data):
    """Handles a player submitting their ordered list for the current 'Order Up!' turn."""
    player_sid = request.sid
    if player_sid not in players or game_state != "order_up_ongoing":
        print(f"WARN: Order Up submission rejected from {player_sid[:4]}. State: {game_state}")
        return

    submitted_list = data.get('ordered_list')

    # Basic validation: is it a list? Does it have the expected number of items?
    # For now, we trust the client sends a list. More robust validation could be added.
    if not isinstance(submitted_list, list):
        emit('message', {'data': 'Invalid submission format.'}, room=player_sid)
        print(f"Order Up! Invalid submission (not a list) from {players[player_sid]['name']}: {submitted_list}")
        return
    
    # Optional: Check if number of items matches expected (e.g., 4)
    # expected_item_count = len(ou_current_question_data['items_in_correct_order'])
    # if len(submitted_list) != expected_item_count:
    #     emit('message', {'data': f'Submission must contain {expected_item_count} items.'}, room=player_sid)
    #     print(f"Order Up! Invalid submission (item count mismatch) from {players[player_sid]['name']}")
    #     return

    if players[player_sid].get('ou_current_submission') is None:
        players[player_sid]['ou_current_submission'] = submitted_list
        player_name = players[player_sid]['name']
        print(f"Order Up! Submission {submitted_list} received from {player_name}({player_sid[:4]})")

        # Update main screen to show player has submitted (optional)
        safe_name_id = player_name.replace('[^a-zA-Z0-9-_]', '_')
        socketio.emit('player_submitted_update', {'name': player_name}, room=main_screen_sid)

        if check_all_submissions_received_ou():
            print("   All 'Order Up!' submissions received.")
            socketio.sleep(0.5) # Brief pause before showing results
            process_order_up_turn_results()
    else:
        emit('message', {'data': 'You already submitted for this question.'}, room=player_sid)
        print(f"Order Up! Duplicate submission attempt from {players[player_sid]['name']}")

def process_order_up_turn_results():
    """Processes submissions, calculates scores, and sends results for an 'Order Up!' turn."""
    global game_state
    print(f"--- Processing Order Up! Turn Results (Index: {ou_current_question_index}) ---")
    if game_state != "order_up_ongoing" or not ou_current_question_data:
        print(f"WARN: Skipping OU results. State: {game_state}, QuestionData: {ou_current_question_data is not None}")
        return

    game_state = "order_up_results_display" # Temp state for showing results

    correct_order = ou_current_question_data['items_in_correct_order']
    turn_results_list = []

    print(f"   Correct Order was: {correct_order}")

    active_players_copy = list(players.items())
    for sid, p_info in active_players_copy:
        player_submission = p_info.get('ou_current_submission')
        was_perfectly_correct = False
        turn_score_for_player = 0

        if player_submission and player_submission == correct_order: # All or nothing
            was_perfectly_correct = True
            turn_score_for_player = 1
        
        if 'round_score' not in p_info: p_info['round_score'] = 0
        p_info['round_score'] += turn_score_for_player
        
        print(f"   - Player: {p_info['name']}, Submission: {player_submission}, Correct: {was_perfectly_correct}, New Round Score: {p_info['round_score']}")

        turn_results_list.append({
            'name': p_info['name'],
            'submission': player_submission if player_submission is not None else ['N', '/', 'A'], # Display something if no submission
            'is_correct': was_perfectly_correct,
            'round_score': p_info['round_score']
        })

    turn_results_list.sort(key=lambda x: (-int(x['is_correct']), x['name'])) # Sort by correct, then name

    results_context = {
        'question_text': ou_current_question_data['question'],
        'correct_order': correct_order,
        'results': turn_results_list,
        'turn': ou_current_question_index + 1,
        'total_turns': ou_actual_turns_this_round
    }
    # NOTE: You will need to create an '_ou_turn_results.html' template
    update_main_screen_html('#results-area', '_ou_turn_results.html', results_context)
    print(f"   Sent 'Order Up!' turn results to main screen.")
    socketio.emit('results_on_main_screen', room=PLAYERS_ROOM)

    turn_results_display_time = 10 # Seconds to show turn results, can be longer for OU
    socketio.sleep(turn_results_display_time)

    if game_state == "order_up_results_display":
        print(f"   Proceeding to next 'Order Up!' turn or end of round.")
        next_order_up_turn()
    else:
        print(f"WARN: Game state changed during OU results sleep ({game_state}). Not proceeding.")

def end_order_up_round():
    """Finalizes the 'Order Up!' round, awards game points, and transitions."""
    global game_state
    game_state = "order_up_results" # Final round results state
    print("\n--- Ending Order Up! Round ---")

    active_players = [(sid, p.get('round_score', 0)) for sid, p in players.items()]
    # Sort by round_score (higher is better), then name
    sorted_by_round = sorted(active_players, key=lambda item: (-item[1], players.get(item[0],{}).get('name','')))
    sorted_sids = [item[0] for item in sorted_by_round]
    print(f"   Order Up! Round Ranks (SID, Score): {sorted_by_round}")

    print(f"   Overall scores BEFORE award_game_points: {overall_game_scores}")
    points_awarded = award_game_points(sorted_sids) # Use existing Stableford helper
    print(f"   Overall scores AFTER award_game_points: {overall_game_scores}")
    print(f"   Points awarded this round: {points_awarded}")

    emit_game_state_update() # Update status bar with new overall scores

    rankings_this_round = []
    for rank, sid in enumerate(sorted_sids):
        if sid in players:
            rankings_this_round.append({
                'rank': rank + 1,
                'name': players[sid]['name'],
                'round_score': players[sid]['round_score'], # Number of perfect orders
                'points_awarded': points_awarded.get(sid, 0)
            })

    current_overall_scores_list = [{'name': p['name'], 'game_score': overall_game_scores.get(sid, 0)}
                                   for sid, p in players.items()]
    current_overall_scores_list.sort(key=lambda x: x['game_score'], reverse=True)

    summary_context = {
        'round_type': ROUND_DISPLAY_NAMES.get('order_up', 'Order Up!'),
        'rankings': rankings_this_round,
        'overall_scores': current_overall_scores_list
    }
    print(f"   Summary Context for Order Up!: {summary_context}")
    update_main_screen_html('#results-area', '_round_summary.html', summary_context) # Reuse existing summary
    print("   Sent 'round_over_summary' HTML for Order Up!.")

    round_summary_display_time = 12
    print(f"   Waiting {round_summary_display_time}s before next game round...")
    socketio.sleep(round_summary_display_time)

    if game_state == "order_up_results":
        start_next_game_round()
    else:
        print(f"WARN: Game state changed during Order Up! summary sleep ({game_state}). Not proceeding.")


# === QUICK PAIRS LOGIC === (Create this new section)

def check_all_submissions_received_qp():
    """Checks if all connected players have submitted their 'Quick Pairs' for the current turn."""
    if not players:
        return True
    return all(p.get('qp_current_submission') is not None for p in players.values())

def setup_quick_pairs_round():
    """Sets up the state for a 'Quick Pairs' round."""
    global game_state, qp_shuffled_questions_this_round, qp_current_question_index
    global qp_actual_turns_this_round, qp_questions, qp_target_turns
    print("--- Setup Quick Pairs Round ---")
    game_state = "quick_pairs_ongoing"

    if not qp_questions:
        print("ERROR: No questions loaded for 'Quick Pairs'. Skipping round.")
        start_next_game_round()
        return

    for sid in players:
        players[sid]['round_score'] = 0 # Points for correct sets of pairs
        players[sid]['qp_current_submission'] = None
        players[sid]['qp_submission_time_ms'] = float('inf') # Reset time for each round

    qp_actual_turns_this_round = min(qp_target_turns, len(qp_questions))
    if qp_actual_turns_this_round == 0: # Should not happen if qp_questions has items
        print("ERROR: No turns to play for 'Quick Pairs'. Skipping round.")
        start_next_game_round()
        return
        
    qp_shuffled_questions_this_round = random.sample(qp_questions, qp_actual_turns_this_round)
    qp_current_question_index = -1

    print(f"Quick Pairs Round starting with {qp_actual_turns_this_round} questions.")
    emit_game_state_update()
    socketio.sleep(0.5)
    next_quick_pairs_turn()

def next_quick_pairs_turn():
    """Advances to the next turn/question in the 'Quick Pairs' round."""
    global game_state, qp_current_question_data, qp_current_question_index
    global qp_shuffled_questions_this_round, qp_actual_turns_this_round, QP_NUM_PAIRS_PER_QUESTION

    qp_current_question_index += 1

    if qp_current_question_index >= qp_actual_turns_this_round:
        end_quick_pairs_round()
        return

    game_state = "quick_pairs_ongoing"
    qp_current_question_data = qp_shuffled_questions_this_round[qp_current_question_index]

    for sid in players: # Reset for the new turn
        players[sid]['qp_current_submission'] = None
        players[sid]['qp_submission_time_ms'] = float('inf') 

    # Prepare the two lists of items for players
    # qp_current_question_data['pairs'] is like [["A1","B1"], ["A2","B2"], ["A3","B3"]]
    list_a_items = [pair[0] for pair in qp_current_question_data['pairs']]
    list_b_items = [pair[1] for pair in qp_current_question_data['pairs']]

    random.shuffle(list_a_items) # Shuffle list A independently
    random.shuffle(list_b_items) # Shuffle list B independently

    print(f"\n-- Quick Pairs Turn {qp_current_question_index + 1}/{qp_actual_turns_this_round} --")
    print(f"   Prompt: {qp_current_question_data['category_prompt']}")
    # For debugging server-side:
    # print(f"   Correct Pairs (Server): {qp_current_question_data['pairs']}")
    # print(f"   Shuffled List A for Players: {list_a_items}")
    # print(f"   Shuffled List B for Players: {list_b_items}")

    main_screen_context = {
        'turn': qp_current_question_index + 1,
        'total_turns': qp_actual_turns_this_round,
        'category_prompt': qp_current_question_data['category_prompt'],
        # Optionally send shuffled lists to main screen if you want audience to see them
        'list_a_items': list_a_items,
        'list_b_items': list_b_items,
        'players_status': [{'name': p['name']} for p in players.values()]
    }
    # NOTE: You will need to create '_qp_turn_display.html'
    update_main_screen_html('#round-content-area', '_qp_turn_display.html', main_screen_context)

    player_payload = {
        'category_prompt': qp_current_question_data['category_prompt'],
        'list_a': list_a_items,
        'list_b': list_b_items,
        'num_pairs_to_make': QP_NUM_PAIRS_PER_QUESTION
    }
    socketio.emit('qp_player_prompt', player_payload, room=PLAYERS_ROOM)
    print("   Sent 'Quick Pairs' prompt and item lists to players.")

@socketio.on('submit_qp_pairs')
def handle_submit_qp_pairs(data):
    """Handles a player submitting their formed pairs for 'Quick Pairs'."""
    player_sid = request.sid
    if player_sid not in players or game_state != "quick_pairs_ongoing":
        print(f"WARN: Quick Pairs submission rejected from {player_sid[:4]}. State: {game_state}")
        return

    submitted_pairs_list = data.get('player_pairs') # e.g., [["France", "Paris"], ["Japan", "Tokyo"], ...]
    time_taken_ms = data.get('time_ms')

    # Basic validation
    if not isinstance(submitted_pairs_list, list) or \
       not all(isinstance(p, list) and len(p) == 2 for p in submitted_pairs_list) or \
       len(submitted_pairs_list) != QP_NUM_PAIRS_PER_QUESTION or \
       time_taken_ms is None or not isinstance(time_taken_ms, (int, float)) or time_taken_ms < 0:
        emit('message', {'data': 'Invalid submission format or data.'}, room=player_sid)
        print(f"QP Invalid submission from {players[player_sid]['name']}: {data}")
        return

    if players[player_sid].get('qp_current_submission') is None: # First submission for this turn
        players[player_sid]['qp_current_submission'] = submitted_pairs_list
        players[player_sid]['qp_submission_time_ms'] = time_taken_ms # Store their completion time
        
        player_name = players[player_sid]['name']
        print(f"QP Submission from {player_name}({player_sid[:4]}): {submitted_pairs_list} in {time_taken_ms}ms")

        safe_name_id = player_name.replace('[^a-zA-Z0-9-_]', '_')
        socketio.emit('player_submitted_update', {'name': player_name}, room=main_screen_sid)

        if check_all_submissions_received_qp():
            print("   All 'Quick Pairs' submissions received.")
            socketio.sleep(0.5)
            process_quick_pairs_turn_results()
    else:
        emit('message', {'data': 'You already submitted for this question.'}, room=player_sid)

def process_quick_pairs_turn_results():
    """Processes submissions, awards points based on correctness and speed."""
    global game_state, qp_current_question_data
    print(f"--- Processing Quick Pairs Turn Results (Index: {qp_current_question_index}) ---")
    if game_state != "quick_pairs_ongoing" or not qp_current_question_data:
        print(f"WARN: Skipping QP results. State: {game_state}, QData: {qp_current_question_data is not None}")
        return

    game_state = "quick_pairs_results_display"

    correct_pairs_set = set(tuple(sorted(p)) for p in qp_current_question_data['pairs'])
    turn_results_list = []
    correct_submitters_times = [] # List of (time_ms, sid) for those who got all pairs right

    for sid, p_info in players.items():
        player_submission = p_info.get('qp_current_submission')
        player_time_ms = p_info.get('qp_submission_time_ms', float('inf'))
        all_pairs_correct = False
        num_correct_player_pairs = 0

        if player_submission and len(player_submission) == QP_NUM_PAIRS_PER_QUESTION:
            player_submission_set = set(tuple(sorted(p)) for p in player_submission)
            if player_submission_set == correct_pairs_set:
                all_pairs_correct = True
                correct_submitters_times.append({'sid': sid, 'time_ms': player_time_ms, 'name': p_info['name']})
                num_correct_player_pairs = QP_NUM_PAIRS_PER_QUESTION # All correct
            else: # Check partial (though we only score all-or-nothing for the 1 point)
                for submitted_pair_tuple in player_submission_set:
                    if submitted_pair_tuple in correct_pairs_set:
                        num_correct_player_pairs +=1
        
        # Points are awarded based on speed bonus later
        print(f"   - Player: {p_info['name']}, AllCorrect: {all_pairs_correct}, Pairs: {num_correct_player_pairs}/{QP_NUM_PAIRS_PER_QUESTION}, Time: {player_time_ms}ms")
        
        turn_results_list.append({
            'name': p_info['name'],
            'all_correct': all_pairs_correct,
            'num_correct_pairs': num_correct_player_pairs, # For display
            'time_ms': player_time_ms if player_submission else '-', # For display
            'points_this_turn': 0 # Will be updated after finding fastest
        })

    # Determine fastest correct player and award points
    fastest_correct_player_sid = None
    if correct_submitters_times:
        correct_submitters_times.sort(key=lambda x: x['time_ms']) # Sort by time, fastest first
        fastest_correct_player_sid = correct_submitters_times[0]['sid']
        print(f"   Fastest correct player: {correct_submitters_times[0]['name']} ({correct_submitters_times[0]['time_ms']}ms)")

        for sid, p_info in players.items():
            if p_info.get('qp_current_submission') and \
               set(tuple(sorted(p)) for p in p_info['qp_current_submission']) == correct_pairs_set:
                turn_score_for_player = 0
                if sid == fastest_correct_player_sid:
                    turn_score_for_player = 2 # 2 points for fastest correct
                    print(f"      awarding 2 pts to {p_info['name']}")
                else:
                    turn_score_for_player = 1 # 1 point for other correct
                    print(f"     awarding 1 pt to {p_info['name']}")
                
                players[sid]['round_score'] += turn_score_for_player
                # Update points_this_turn in turn_results_list for display
                for res_item in turn_results_list:
                    if res_item['name'] == p_info['name']:
                        res_item['points_this_turn'] = turn_score_for_player
                        break
    
    # Update round_score in turn_results_list for final display
    for res_item in turn_results_list:
        player_entry = next((p for s, p in players.items() if p['name'] == res_item['name']), None)
        if player_entry:
            res_item['round_score'] = player_entry['round_score']


    # Sort for display (e.g., by points this turn, then by correctness, then by name)
    turn_results_list.sort(key=lambda x: (-x['points_this_turn'], -int(x['all_correct']), x['name']))

    results_context = {
        'category_prompt': qp_current_question_data['category_prompt'],
        'correct_pairs': qp_current_question_data['pairs'], # List of [itemA, itemB]
        'results': turn_results_list,
        'turn': qp_current_question_index + 1,
        'total_turns': qp_actual_turns_this_round,
        'num_pairs_per_question': QP_NUM_PAIRS_PER_QUESTION
    }
    # NOTE: You will need to create '_qp_turn_results.html'
    update_main_screen_html('#results-area', '_qp_turn_results.html', results_context)
    socketio.emit('results_on_main_screen', room=PLAYERS_ROOM)

    turn_results_display_time = 10
    socketio.sleep(turn_results_display_time)

    if game_state == "quick_pairs_results_display":
        next_quick_pairs_turn()
    else:
        print(f"WARN: Game state changed during QP results sleep ({game_state}).")


def end_quick_pairs_round():
    """Finalizes the 'Quick Pairs' round."""
    global game_state
    game_state = "quick_pairs_results" # Final round results state
    print("\n--- Ending Quick Pairs Round ---")

    active_players = [(sid, p.get('round_score', 0)) for sid, p in players.items()]
    sorted_by_round = sorted(active_players, key=lambda item: (-item[1], players.get(item[0],{}).get('name',''))) # Higher score is better
    sorted_sids = [item[0] for item in sorted_by_round]

    points_awarded = award_game_points(sorted_sids) # Use existing Stableford
    emit_game_state_update()

    rankings_this_round = []
    for rank, sid in enumerate(sorted_sids):
        if sid in players:
            rankings_this_round.append({
                'rank': rank + 1,
                'name': players[sid]['name'],
                'round_score': players[sid]['round_score'], # Total points from correct pairs
                'points_awarded': points_awarded.get(sid, 0)
            })

    current_overall_scores_list = [{'name': p['name'], 'game_score': overall_game_scores.get(sid, 0)}
                                   for sid, p in players.items()]
    current_overall_scores_list.sort(key=lambda x: x['game_score'], reverse=True)

    summary_context = {
        'round_type': ROUND_DISPLAY_NAMES.get('quick_pairs', 'Quick Pairs'),
        'rankings': rankings_this_round,
        'overall_scores': current_overall_scores_list
    }
    update_main_screen_html('#results-area', '_round_summary.html', summary_context)

    round_summary_display_time = 12
    socketio.sleep(round_summary_display_time)

    if game_state == "quick_pairs_results":
        start_next_game_round()
    else:
        print(f"WARN: Game state changed during QP summary sleep ({game_state}).")

# === TRUE OR FALSE LOGIC ===

def check_all_guesses_received_tf():
    if not players: return True
    return all(p.get('tf_current_guess') is not None for p in players.values())

def setup_true_or_false_round():
    global game_state, tf_shuffled_questions_this_round, tf_current_question_index, tf_actual_turns_this_round
    print("--- Setup True or False Round ---")
    game_state = "true_or_false_ongoing"

    if not tf_questions:
        print("ERROR: No questions for True or False. Skipping.")
        start_next_game_round()
        return

    for sid in players:
        players[sid]['round_score'] = 0
        players[sid]['tf_current_guess'] = None

    tf_actual_turns_this_round = min(tf_target_turns, len(tf_questions))
    tf_shuffled_questions_this_round = random.sample(tf_questions, tf_actual_turns_this_round)
    tf_current_question_index = -1

    print(f"True or False Round starting with {tf_actual_turns_this_round} questions.")
    emit_game_state_update()
    socketio.sleep(0.5)
    next_true_or_false_turn()

def next_true_or_false_turn():
    global game_state, tf_current_question, tf_current_question_index

    tf_current_question_index += 1

    if tf_current_question_index >= tf_actual_turns_this_round:
        end_true_or_false_round()
        return

    game_state = "true_or_false_ongoing"
    tf_current_question = tf_shuffled_questions_this_round[tf_current_question_index]

    for sid in players:
        players[sid]['tf_current_guess'] = None

    print(f"\n-- TF Turn {tf_current_question_index + 1}/{tf_actual_turns_this_round} --")
    print(f"   Statement: {tf_current_question['statement']}")
    print(f"   Correct: {tf_current_question['correct_answer']}")

    main_screen_context = {
        'turn': tf_current_question_index + 1,
        'total_turns': tf_actual_turns_this_round,
        'statement': tf_current_question['statement'],
        'players_status': [{'name': p['name']} for p in players.values()]
    }
    update_main_screen_html('#round-content-area', '_true_or_false_turn_display.html', main_screen_context)

    player_payload = {'statement': tf_current_question['statement']}
    socketio.emit('true_or_false_player_prompt', player_payload, room=PLAYERS_ROOM)

@socketio.on('submit_true_or_false_guess')
def handle_submit_tf_guess(data):
    player_sid = request.sid
    if player_sid not in players or game_state != "true_or_false_ongoing": return

    guess = data.get('guess')
    if guess is None or not isinstance(guess, bool):
        print(f"Invalid TF guess from {players[player_sid]['name']}: {guess}")
        return

    if players[player_sid].get('tf_current_guess') is None:
        players[player_sid]['tf_current_guess'] = guess
        player_name = players[player_sid]['name']
        print(f"TF Guess '{guess}' received from {player_name}")
        socketio.emit('player_submitted_update', {'name': player_name}, room=main_screen_sid)

        if check_all_guesses_received_tf():
            print("   All TF guesses received.")
            socketio.sleep(0.5)
            process_true_or_false_turn_results()

def process_true_or_false_turn_results():
    global game_state
    if game_state != "true_or_false_ongoing": return
    game_state = "tf_results_display"
    
    correct_answer = tf_current_question['correct_answer']
    turn_results_list = []

    for sid, p_info in players.items():
        guess = p_info.get('tf_current_guess')
        was_correct = (guess == correct_answer)
        
        if was_correct:
            p_info['round_score'] += 1

        turn_results_list.append({
            'name': p_info['name'],
            'guess_text': "True" if guess else "False" if guess is not None else "N/A",
            'is_correct': was_correct,
            'round_score': p_info['round_score']
        })
    
    turn_results_list.sort(key=lambda x: (-int(x['is_correct']), x['name']))

    results_context = {
        'statement': tf_current_question['statement'],
        'correct_answer_text': "TRUE" if correct_answer else "FALSE",
        'results': turn_results_list,
    }
    update_main_screen_html('#results-area', '_true_or_false_turn_results.html', results_context)
    socketio.emit('results_on_main_screen', room=PLAYERS_ROOM)

    socketio.sleep(6) # Show results for 6 seconds
    if game_state == "tf_results_display":
        next_true_or_false_turn()

def end_true_or_false_round():
    global game_state
    game_state = "true_or_false_results"
    print("\n--- Ending True or False Round ---")

    active_players = [(sid, p.get('round_score', 0)) for sid, p in players.items()]
    sorted_by_round = sorted(active_players, key=lambda item: (-item[1], players.get(item[0], {}).get('name','')))
    sorted_sids = [item[0] for item in sorted_by_round]
    
    points_awarded = award_game_points(sorted_sids)
    emit_game_state_update()

    rankings_this_round = []
    for rank, sid in enumerate(sorted_sids):
        if sid in players:
            rankings_this_round.append({
                'rank': rank + 1,
                'name': players[sid]['name'],
                'round_score': players[sid]['round_score'],
                'points_awarded': points_awarded.get(sid, 0)
            })

    current_overall_scores_list = [{'name': p['name'], 'game_score': overall_game_scores.get(sid, 0)} for sid, p in players.items()]
    current_overall_scores_list.sort(key=lambda x: x['game_score'], reverse=True)

    summary_context = {
        'round_type': ROUND_DISPLAY_NAMES.get('true_or_false', 'True or False'),
        'rankings': rankings_this_round,
        'overall_scores': current_overall_scores_list
    }
    update_main_screen_html('#results-area', '_round_summary.html', summary_context)

    socketio.sleep(12)
    if game_state == "true_or_false_results":
        start_next_game_round()

# === MAIN EXECUTION ===
if __name__ == '__main__':
    print("Loading round data...");
    load_guess_the_age_data()
    load_guess_the_year_data()
    load_who_didnt_do_it_data()
    load_order_up_data()
    load_quick_pairs_data()
    load_true_or_false_data()
    print("Starting Flask-SocketIO server..."); use_debug = False
    socketio.run(app, host='0.0.0.0', port=5000, debug=use_debug)
    print("Server stopped.")