"""Last One Standing regression tests.

Run: python -m unittest discover -s tests -p 'test_los.py'
"""
import unittest
from collections import Counter
from contextlib import contextmanager, nullcontext
from types import SimpleNamespace
from last_one_standing import LastOneStanding


class FakeSocket:
    def __init__(self):
        self.events = []
        self.tasks = []

    def emit(self, event, data=None, room=None):
        self.events.append((event, data, room))

    def start_background_task(self, fn):
        self.tasks.append(fn)

    def sleep(self, seconds):
        pass


class SurvivalTests(unittest.TestCase):
    def setUp(self):
        self.socket = FakeSocket()
        players = {s: {'name': s.upper(), 'round_score': 0} for s in ('a', 'b', 'c')}
        self.host = SimpleNamespace(
            game_state="last_one_standing_ongoing",
            players=players,
            overall_game_scores={s: 0 for s in players},
            main_screen_sid=None,
            PLAYERS_ROOM='players',
            app=SimpleNamespace(app_context=nullcontext),
            socketio=self.socket,
            render_template=lambda *args, **kw: '',
            LOS_REVEAL_SECONDS=1,
            LOS_SUMMARY_SECONDS=1,
            emit_game_state_update=lambda: None,
            update_main_screen_html=lambda *args: None,
            start_next_game_round=lambda: None,
        )
        self.round = LastOneStanding(self.host)
        self.round.run_id = 'running'
        self.round.token = 'q1'
        self.round.phase = 'question'
        self.round.number = 1
        self.round.lives = {s: 3 for s in players}
        self.round.question = {
            'question': 'Example', 'options': ['yes', 'no', 'maybe', 'other'],
            'correct_option': 0, 'correct_answer': 'yes', 'explanation': 'Example',
        }
        self.round.answer_order = [2, 1, 0, 3]
        self.round.answers = {}

    def respond(self, answers):
        for sid, choice in answers.items():
            self.round.answer(sid, {'token': self.round.token, 'choice': choice})

    def test_fixed_difficulty_staircase(self):
        self.assertEqual([self.set_difficulty_number(n) for n in range(1, 19)],
                         [1, 1, 2, 2, 3, 3, 4, 4] + [5] * 10)

    def set_difficulty_number(self, n):
        self.round.number = n
        return self.round.difficulty()

    def test_player_answer_is_mapped_through_shuffled_options(self):
        self.respond({'a': 2, 'b': 1, 'c': 2})
        self.assertEqual(self.round.phase, 'reveal')
        self.assertEqual(self.round.lives, {'a': 3, 'b': 2, 'c': 3})
        self.assertEqual(self.round.outcomes, {'a': True, 'b': False, 'c': True})
        self.assertFalse(any('correct' in o for o in self.round.snapshot()['options']))

    def test_no_advance_until_all_survivors_answer(self):
        self.round.lives['c'] = 0
        self.respond({'a': 2})
        self.assertEqual(self.round.phase, 'question')
        self.respond({'b': 1})
        self.assertEqual(self.round.phase, 'reveal')
        self.assertNotIn('c', self.round.outcomes)

    def test_simultaneous_final_elimination_is_a_tie(self):
        self.round.number = 8
        self.round.lives = {'a': 1, 'b': 1, 'c': 0}
        self.round.eliminated_on = {'c': 5}
        self.respond({'a': 1, 'b': 1})
        self.assertEqual(self.round.lives['a'], 0)
        self.assertEqual(self.round.eliminated_on['a'], 8)
        self.assertEqual(self.round.eliminated_on['b'], 8)
        self.assertEqual(self.round.active(), [])
        self.round.after_reveal()
        self.assertEqual(self.round.phase, 'summary')
        self.assertEqual(self.host.players['a']['round_score'], self.host.players['b']['round_score'])
        self.assertGreater(self.host.players['a']['round_score'], self.host.players['c']['round_score'])

    def test_sole_survivor_ranks_above_latest_eliminated(self):
        self.round.number = 11
        self.round.lives = {'a': 2, 'b': 1, 'c': 0}
        self.round.eliminated_on = {'c': 6}
        self.respond({'a': 2, 'b': 1})
        self.round.after_reveal()
        self.assertEqual(self.round.phase, 'summary')
        self.assertGreater(self.host.players['a']['round_score'], self.host.players['b']['round_score'])

    def test_background_transition_has_app_context(self):
        active = {'yes': False}

        @contextmanager
        def context():
            active['yes'] = True
            yield
            active['yes'] = False

        self.host.app.app_context = context
        seen = []

        def callback():
            self.assertTrue(active['yes'])
            seen.append('ran')

        self.round.phase = 'reveal'
        self.round.delay(1, callback)
        self.socket.tasks.pop()()
        self.assertEqual(seen, ['ran'])
        self.assertFalse(active['yes'])

    def test_sid_migration_preserves_lives_and_answer(self):
        self.round.lives['a'] = 1
        self.round.answers['a'] = 2
        self.round.migrate('a', 'new-a')
        self.assertEqual(self.round.lives['new-a'], 1)
        self.assertEqual(self.round.answers['new-a'], 2)
        self.assertNotIn('a', self.round.lives)


if __name__ == '__main__':
    unittest.main()
