"""Exercise the real Flask/Socket.IO handlers without real-time presentation delays."""
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ROUND_TYPES = [
    'guess_the_age', 'guess_the_year', 'who_didnt_do_it', 'order_up',
    'quick_pairs', 'true_or_false', 'tap_the_pic', 'the_top_three',
    'higher_or_lower', 'averagers_assemble',
]


class Session:
    def __init__(self, game, count):
        self.g = game
        self.main = game.socketio.test_client(game.app)
        self.main.emit('register_main_screen')
        self.clients = [self.join(i) for i in range(count)]

    def join(self, i, name=None):
        client = self.g.socketio.test_client(self.g.app)
        client.emit('register_player', {'name': name or f'Player{i}', 'pid': f'pid{i}'})
        return client

    def sid(self, client):
        return self.g.socketio.server.manager.sid_from_eio_sid(client.eio_sid, '/')

    def client_for(self, sid):
        return next(c for c in self.clients if self.sid(c) == sid)

    def finish_sequence(self, event):
        self.main.emit(event, self.g.main_screen_sequence['payload'].copy())

    def start(self, key):
        self.g.GAME_ROUNDS_TOTAL = 1
        self.g.AVAILABLE_ROUND_TYPES = [key]
        self.main.emit('start_game_request')
        self.finish_sequence('game_intro_finished')
        self.finish_sequence('how_to_play_finished')

    def draft(self):
        while self.g.aa_round_phase == 'selection':
            self.client_for(self.g.aa_current_picker_sid).emit(
                'submit_team_pick', {'picked_sid': self.g.aa_unpicked_players[1]})

    def answer(self, key, index=0, correct=True):
        g = self.g
        answers = {
            'guess_the_age': lambda: ('submit_gta_guess', {'guess': g.gta_current_celebrity['age'] + (0 if correct else 1)}),
            'guess_the_year': lambda: ('submit_gty_guess', {'guess': g.gty_current_question['year'] + (0 if correct else 1)}),
            'who_didnt_do_it': lambda: ('submit_wddi_guess', {'guess_text': g.wddi_current_question['correct_answer'] if correct else next(x for x in g.wddi_current_shuffled_options if x != g.wddi_current_question['correct_answer'])}),
            'order_up': lambda: ('submit_ou_list', {'ordered_list': g.ou_current_question_data['items_in_correct_order'] if correct else g.ou_current_question_data['items_in_correct_order'][::-1]}),
            'quick_pairs': lambda: ('submit_qp_pairs', {'player_pairs': g.qp_current_question_data['pairs'] if correct else [[a, 'wrong'] for a, b in g.qp_current_question_data['pairs']], 'time_ms': 1000 + index}),
            'true_or_false': lambda: ('submit_true_or_false_guess', {'guess': g.tf_current_question['correct_answer'] if correct else not g.tf_current_question['correct_answer']}),
            'tap_the_pic': lambda: ('submit_ttp_guess', {'guess': g.ttp_current_question['correct_answer'] if correct else (g.ttp_current_question['correct_answer'] % g.ttp_current_question['num_options']) + 1}),
            'the_top_three': lambda: ('submit_top_three_guess', {'guess': g.ttt_current_question['correct_answers'] if correct else [x for x in g.ttt_current_question['options'] if x not in g.ttt_current_question['correct_answers']][:3]}),
            'averagers_assemble': lambda: ('submit_aa_guess', {'guess': g.aa_current_question['answer'] + (0 if correct else 100)}),
        }
        return answers[key]()

    def play_round(self, key):
        g = self.g
        if key == 'averagers_assemble':
            self.draft()
        if key == 'higher_or_lower':
            for _ in range(g.hol_actual_turns_this_round):
                submitter = g.hol_current_submitter_sid
                self.client_for(submitter).emit('submit_hol_guess', {'guess': g.hol_current_question['answer'] - 1})
                for client in self.clients:
                    if self.sid(client) != submitter:
                        client.emit('submit_hol_guess', {'guess': 'Higher'})
        else:
            for i, client in enumerate(self.clients):
                client.emit(*self.answer(key, i))

    def reconnect_main(self):
        self.main.disconnect()
        self.main = self.g.socketio.test_client(self.g.app)
        self.main.emit('register_main_screen')
        return self.main.get_received()


@pytest.fixture
def session_factory(monkeypatch):
    monkeypatch.chdir(ROOT)
    def factory(count=3):
        spec = importlib.util.spec_from_file_location('hff_under_test', ROOT / 'app.py')
        game = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, game)
        spec.loader.exec_module(game)
        game.socketio.sleep = lambda *_: None
        game.random.seed(1234)
        for key, value in vars(game).copy().items():
            if key.startswith('load_') and callable(value):
                value()
        return Session(game, count)
    return factory


@pytest.mark.parametrize('count', range(2, 9))
@pytest.mark.parametrize('key', ROUND_TYPES)
def test_all_rounds_complete_and_score(session_factory, count, key):
    s = session_factory(count)
    s.start(key)
    s.play_round(key)
    assert s.g.game_state == 'overall_game_over'
    assert all(score > 0 for score in s.g.overall_game_scores.values())
    if key in ('guess_the_age', 'guess_the_year'):
        assert all(p['round_score'] == 0 for p in s.g.players.values())
    elif key == 'quick_pairs':
        assert [s.g.players[s.sid(c)]['round_score'] for c in s.clients] == [2] + [1] * (count - 1)
    elif key != 'higher_or_lower':
        assert all(p['round_score'] == (3 if key == 'the_top_three' else 1) for p in s.g.players.values())


@pytest.mark.parametrize('key', [key for key in ROUND_TYPES if key != 'higher_or_lower'])
def test_correct_answer_beats_incorrect_answer(session_factory, key):
    s = session_factory(2)
    s.start(key)
    s.clients[0].emit(*s.answer(key, correct=True))
    s.clients[1].emit(*s.answer(key, correct=False))
    assert s.g.game_state == 'overall_game_over'
    assert [s.g.overall_game_scores[s.sid(c)] for c in s.clients] == [3, 1]


@pytest.mark.parametrize('count', [2, 4, 8])
def test_ten_round_game_and_replay(session_factory, count):
    s = session_factory(count)
    s.g.AVAILABLE_ROUND_TYPES = ROUND_TYPES.copy()
    s.main.emit('start_game_request')
    s.finish_sequence('game_intro_finished')
    for _ in range(10):
        key = s.g.selected_rounds_for_game[s.g.current_game_round_num - 1]
        s.finish_sequence('how_to_play_finished')
        s.play_round(key)
    assert s.g.game_state == 'overall_game_over'
    s.main.emit('request_reset_game')
    assert s.g.game_state == 'waiting'
    assert s.g.current_game_round_num == 0
    assert s.g.main_screen_view is None
    assert s.g.main_screen_sequence is None
    assert all(score == 0 for score in s.g.overall_game_scores.values())
    s.main.emit('start_game_request')
    s.finish_sequence('game_intro_finished')
    s.finish_sequence('how_to_play_finished')
    assert s.g.game_state.endswith('_ongoing')


@pytest.mark.parametrize('key', ROUND_TYPES)
def test_main_screen_restores_question_without_advancing(session_factory, key):
    s = session_factory()
    s.start(key)
    view, state = s.g.main_screen_view.copy(), s.g.game_state
    received = s.reconnect_main()
    assert any(e['name'] == 'update_html' and e['args'][0] == view for e in received)
    assert s.g.game_state == state


@pytest.mark.parametrize('key', ROUND_TYPES)
def test_player_reconnect_preserves_identity_and_score(session_factory, key):
    s = session_factory()
    s.start(key)
    old = s.sid(s.clients[0])
    status_id = s.g.players[old]['status_id']
    s.g.overall_game_scores[old] = 7
    s.clients[0].disconnect()
    replacement = s.join(0)
    new = s.sid(replacement)
    assert old not in s.g.players
    assert len(s.g.players) == 3
    assert s.g.players[new]['status_id'] == status_id
    assert s.g.overall_game_scores[new] == 7
    assert any(e['name'].endswith('_prompt') for e in replacement.get_received())
    s.clients[0] = replacement
    s.play_round(key)
    assert s.g.game_state == 'overall_game_over'


@pytest.mark.parametrize('phase', ['game_intro', 'explainer', 'finale'])
def test_main_screen_restores_sequences_and_rejects_stale_callbacks(session_factory, phase):
    s = session_factory()
    s.main.emit('start_game_request')
    intro_token = s.g.main_screen_sequence['payload'].copy()
    if phase != 'game_intro':
        s.finish_sequence('game_intro_finished')
    if phase == 'finale':
        s.g.GAME_ROUNDS_TOTAL = 1
        s.finish_sequence('how_to_play_finished')
        s.play_round('averagers_assemble')
    expected = s.g.main_screen_sequence.copy()
    received = s.reconnect_main()
    event = next(e for e in received if e['name'] == expected['event'])
    assert event['args'][0]['restored'] is True
    assert event['args'][0]['sequence_id'] == expected['payload']['sequence_id']
    if phase == 'game_intro':
        s.finish_sequence('game_intro_finished')
        current = s.g.main_screen_sequence.copy()
        s.main.emit('game_intro_finished', intro_token)
        assert s.g.main_screen_sequence == current
    elif phase == 'explainer':
        s.main.emit('how_to_play_finished', intro_token)
        assert s.g.main_screen_sequence == expected
        s.finish_sequence('how_to_play_finished')
        question = s.g.aa_current_question
        s.main.emit('how_to_play_finished', expected['payload'])
        assert s.g.aa_current_question is question
    else:
        s.main.emit('request_reset_game')
        assert s.g.game_state == 'waiting'


def test_status_restore_and_arbitrary_duplicate_names(session_factory):
    s = session_factory()
    for i, name in enumerate(['Adam H', 'Adam H', "Zoë & O'Neil"]):
        s.clients[i].emit('register_player', {'name': name, 'pid': f'pid{i}'})
    s.start('true_or_false')
    s.main.get_received()
    first_sid = s.sid(s.clients[0])
    first_id = s.g.players[first_sid]['status_id']
    ids = [p['status_id'] for p in s.g.players.values()]
    assert len(set(ids)) == 3
    assert all('id="player-status-' + ident + '"' in s.g.main_screen_view['html'] for ident in ids)
    s.clients[0].emit('submit_true_or_false_guess', {'guess': False})
    received = s.main.get_received()
    assert [e['args'][0] for e in received if e['name'] == 'player_submitted_update'] == [{'status_id': first_id}]
    received = s.reconnect_main()
    assert [e['args'][0] for e in received if e['name'] == 'player_submitted_update'] == [{'status_id': first_id}]


@pytest.mark.parametrize('count', [4, 5, 6, 7, 8])
def test_draft_refreshes_after_candidate_reconnect(session_factory, count):
    s = session_factory(count)
    s.start('averagers_assemble')
    picker = s.client_for(s.g.aa_current_picker_sid)
    old = s.g.aa_unpicked_players[1]
    i = next(i for i, c in enumerate(s.clients) if s.sid(c) == old)
    picker.get_received()
    s.clients[i].disconnect()
    s.clients[i] = s.join(i)
    new = s.sid(s.clients[i])
    events = picker.get_received()
    prompt = next(e for e in events if e['name'] == 'aa_pick_teammate_prompt')
    choices = [p['sid'] for p in prompt['args'][0]['players_to_choose_from']]
    assert new in choices and old not in choices
    picker.emit('submit_team_pick', {'picked_sid': old})
    assert not s.g.aa_teams
    assert any(e['name'] == 'aa_pick_teammate_prompt' for e in picker.get_received())
    picker.emit('submit_team_pick', {'picked_sid': new})
    s.draft()
    assert sorted(sid for t in s.g.aa_teams for sid in t['members']) == sorted(s.g.players)


def test_draft_picker_can_reconnect(session_factory):
    s = session_factory(4)
    s.start('averagers_assemble')
    i = next(i for i, c in enumerate(s.clients) if s.sid(c) == s.g.aa_current_picker_sid)
    s.clients[i].disconnect()
    s.clients[i] = s.join(i)
    assert s.g.aa_current_picker_sid == s.sid(s.clients[i])
    s.draft()
    assert s.g.aa_round_phase == 'gameplay'


@pytest.mark.parametrize('key', ROUND_TYPES)
def test_new_players_wait_for_lobby_and_do_not_affect_scoring(session_factory, key):
    s = session_factory()
    s.start(key)
    late = s.join(99)
    assert len(s.g.players) == 3
    assert 'pid99' not in s.g.pid_to_sid
    assert any(e['name'] == 'registration_rejected' for e in late.get_received())
    s.play_round(key)
    assert s.g.game_state == 'overall_game_over'
    s.main.emit('request_reset_game')
    late.emit('register_player', {'name': 'Late', 'pid': 'pid99'})
    assert s.sid(late) in s.g.players


def test_lobby_places_released_but_midgame_identity_retained(session_factory):
    s = session_factory(8)
    rejected = s.join(9)
    assert any(e['name'] == 'registration_rejected' for e in rejected.get_received())
    for client in s.clients:
        client.disconnect()
    assert not s.g.players and not s.g.pid_to_sid and not s.g.sid_to_pid
    s.clients = [s.join(0), s.join(1)]
    s.start('averagers_assemble')
    sid = s.sid(s.clients[0])
    s.clients[0].disconnect()
    assert sid in s.g.players and s.g.players[sid]['connected'] is False
    s.clients[0] = s.join(0)
    s.play_round('averagers_assemble')
    sid = s.sid(s.clients[0])
    s.clients[0].disconnect()
    s.main.emit('request_reset_game')
    assert sid not in s.g.players
    assert len(s.g.players) == 1


def test_averagers_compares_exact_means_and_displays_decimals(session_factory):
    s = session_factory(4)
    s.start('averagers_assemble')
    s.draft()
    s.g.aa_current_question = {'question': 'Boundary', 'answer': 10}
    s.main.get_received()
    for team, values in zip(s.g.aa_teams, [[10, 11], [10, 10]]):
        for sid, value in zip(team['members'], values):
            s.client_for(sid).emit('submit_aa_guess', {'guess': value})
    assert [[s.g.players[sid]['round_score'] for sid in t['members']] for t in s.g.aa_teams] == [[0, 0], [1, 1]]
    html = ''.join(e['args'][0]['html'] for e in s.main.get_received() if e['name'] == 'update_html')
    assert '10.5' in html


def test_averagers_exact_tie_still_awards_both_teams(session_factory):
    s = session_factory(4)
    s.start('averagers_assemble')
    s.draft()
    s.g.aa_current_question = {'question': 'Tie', 'answer': 10}
    for team, values in zip(s.g.aa_teams, [[9, 10], [10, 11]]):
        for sid, value in zip(team['members'], values):
            s.client_for(sid).emit('submit_aa_guess', {'guess': value})
    assert all(p['round_score'] == 1 for p in s.g.players.values())


def test_http_assets_and_template_compilation(session_factory):
    s = session_factory()
    http = s.g.app.test_client()
    for path in ['/', '/main'] + ['/' + p.relative_to(ROOT).as_posix() for p in (ROOT / 'static').rglob('*') if p.is_file()]:
        assert http.get(path).status_code == 200, path
    for path in (ROOT / 'templates').glob('*.html'):
        s.g.app.jinja_env.get_template(path.name)


def test_development_configuration_unchanged(session_factory):
    s = session_factory()
    assert s.g.AVAILABLE_ROUND_TYPES == ['averagers_assemble']
    assert s.g.GAME_ROUNDS_TOTAL == 10
    assert all(getattr(s.g, prefix + '_target_turns') == 1 for prefix in ['gta', 'gty', 'wddi', 'ou', 'qp', 'tf', 'ttp', 'ttt', 'hol', 'aa'])


def test_question_and_finale_generated_while_main_screen_offline_are_restored(session_factory):
    s = session_factory()
    s.start('averagers_assemble')
    s.main.disconnect()
    s.play_round('averagers_assemble')
    assert s.g.game_state == 'overall_game_over'
    view = s.g.main_screen_view.copy()
    s.main = s.g.socketio.test_client(s.g.app)
    s.main.emit('register_main_screen')
    events = s.main.get_received()
    assert any(e['name'] == 'update_html' and e['args'][0] == view for e in events)
    assert any(e['name'] == 'start_game_over_sequence' for e in events)


def test_main_screen_reconnect_during_result_countdown_keeps_score_once(session_factory):
    s = session_factory()
    s.start('true_or_false')
    restored = []
    def during_wait(seconds):
        if s.g.game_state == 'tf_results_display' and not restored:
            expected = s.g.main_screen_view.copy()
            events = s.reconnect_main()
            assert any(e['name'] == 'update_html' and e['args'][0] == expected for e in events)
            restored.append(True)
    s.g.socketio.sleep = during_wait
    s.play_round('true_or_false')
    assert restored == [True]
    assert s.g.game_state == 'overall_game_over'
    assert all(p['round_score'] == 1 for p in s.g.players.values())
    assert all(score == 2.3 for score in s.g.overall_game_scores.values())
