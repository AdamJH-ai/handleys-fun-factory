"""Small server-state regression checks for Would I Lie to You.

Run from repo root: python -m unittest discover -s tests -p 'test_wilty.py'
"""
import unittest
from contextlib import contextmanager, nullcontext
from types import SimpleNamespace
from would_i_lie_to_you import WouldILikeToYou, normalise


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


class BluffRoundTests(unittest.TestCase):
    def setUp(self):
        self.socket = FakeSocket()
        self.host = SimpleNamespace(
            game_state='would_i_lie_to_you_ongoing',
            players={s: {'name': s.upper(), 'round_score': 0} for s in ('a', 'b', 'c')},
            main_screen_sid=None,
            socketio=self.socket,
            app=SimpleNamespace(app_context=nullcontext),
            render_template=lambda *args, **kwargs: '',
            WILTY_TIMINGS={'lie_reveal': 1, 'truth_reveal': 1, 'question_scores': 1, 'round_summary': 1},
            PLAYERS_ROOM='players',
        )
        self.round = WouldILikeToYou(self.host)
        self.round.run_id = 'test'
        self.round.token = 'first'
        self.round.phase = 'bluff'
        self.round.question_index = 0
        self.round.questions = [dict(question='Which?', answer='The Moon', accepted_answers=['moon'], house_bluffs=['Saturn', 'Earth', 'Jupiter'])]
        self.round.question = self.round.questions[0]

    def test_normalise(self):
        self.assertEqual(normalise(' The  MOON!!! '), 'the moon')
        self.assertEqual(normalise('Mars'), normalise('mArS!!'))

    def test_truth_rejected_and_reentered(self):
        self.round.submit_bluff('a', {'token': 'first', 'text': 'MOON!'})
        self.assertNotIn('a', self.round.bluffs)
        self.assertTrue(any(name == 'wilty_error' and msg.get('truth') for name, msg, _ in self.socket.events))
        self.round.submit_bluff('a', {'token': 'first', 'text': 'Mars'})
        self.assertEqual(self.round.bluffs['a'], 'Mars')

    def test_duplicate_bluffs_full_points_to_each_author(self):
        self.round.submit_bluff('a', {'token': 'first', 'text': 'Mars'})
        self.round.submit_bluff('b', {'token': 'first', 'text': 'MARS!'})
        self.round.submit_bluff('c', {'token': 'first', 'text': 'Venus'})
        self.assertEqual(self.round.phase, 'vote')
        self.assertGreaterEqual(len(self.round.options), 4)
        duplicate = next(o for o in self.round.options if set(o['authors']) == {'a', 'b'})
        truth = next(o for o in self.round.options if o['truth'])
        token = self.round.token
        self.round.submit_vote('a', {'token': token, 'option_id': duplicate['id']})
        self.assertNotIn('a', self.round.votes)  # Cannot vote for own lie.
        self.round.submit_vote('a', {'token': token, 'option_id': truth['id']})
        self.round.submit_vote('b', {'token': token, 'option_id': truth['id']})
        self.round.submit_vote('c', {'token': token, 'option_id': duplicate['id']})
        self.assertEqual(self.round.phase, 'reveal')
        self.assertEqual(self.host.players['a']['round_score'], 3)
        self.assertEqual(self.host.players['b']['round_score'], 3)
        self.assertEqual(self.host.players['c']['round_score'], 0)
        self.assertTrue(self.round.revealed[-1]['truth'])

    def test_background_transition_has_flask_app_context(self):
        """Regression: timed end-of-game rendering needs Flask context."""
        active_context = {'yes': False}

        @contextmanager
        def app_context():
            active_context['yes'] = True
            try:
                yield
            finally:
                active_context['yes'] = False

        self.host.app.app_context = app_context
        calls = []
        def render_final_screen():
            self.assertTrue(active_context['yes'])
            calls.append('rendered')

        self.round.phase = 'summary'
        self.round.delay('round_summary', render_final_screen)
        self.assertEqual(len(self.socket.tasks), 1)
        self.assertFalse(active_context['yes'])
        self.socket.tasks.pop()()
        self.assertEqual(calls, ['rendered'])
        self.assertFalse(active_context['yes'])



if __name__ == '__main__':
    unittest.main()
