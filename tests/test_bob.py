"""Bank or Bust tests: run with python -m unittest discover -s tests -p 'test_bob.py'."""
import unittest
from contextlib import contextmanager, nullcontext
from types import SimpleNamespace
from bank_or_bust import BankOrBust


class FakeSocket:
    def __init__(self):
        self.events = []
        self.tasks = []

    def emit(self, name, data=None, room=None):
        self.events.append((name, data, room))

    def start_background_task(self, task):
        self.tasks.append(task)

    def sleep(self, seconds):
        pass


class BankOrBustTests(unittest.TestCase):
    def setUp(self):
        self.socket = FakeSocket()
        self.context_active = False
        players = {sid: {"name": sid.upper(), "round_score": 0} for sid in ("a", "b", "c")}
        self.host = SimpleNamespace(
            game_state="bank_or_bust_ongoing",
            players=players,
            overall_game_scores={sid: 0 for sid in players},
            main_screen_sid="tv",
            PLAYERS_ROOM="players",
            socketio=self.socket,
            app=SimpleNamespace(app_context=nullcontext),
            render_template=lambda *a, **k: "",
            update_main_screen_html=lambda *a, **k: None,
            emit_game_state_update=lambda: None,
            start_next_game_round=lambda: None,
            award_game_points=lambda order: {sid: len(order) - i for i, sid in enumerate(order)},
            BOB_DECISION_REVEAL_SECONDS=1,
            BOB_ANSWER_REVEAL_SECONDS=1,
            BOB_SUMMARY_SECONDS=1,
            BOB_QUESTIONS_PER_ROUND=10,
        )
        self.game = BankOrBust(self.host)
        self.game.run_id = "run"
        self.game.token = "secret"
        self.game.count = 10
        self.game.number = 1
        self.game.phase = "question"
        self.game.bank = {"a": 0, "b": 0, "c": 0}
        self.game.pot = {"a": 0, "b": 0, "c": 0}
        self.game.question = {
            "question": "Who?", "options": ["a","b","c","d","e","f"],
            "correct_option": 2, "correct_answer": "c", "explanation": "Because."}
        self.game.answer_order = [4, 2, 3, 1, 5, 0]

    def vote(self, sid, choice):
        self.game.choose(sid, {"token": self.game.token, "choice": choice})

    def answer(self, sid, choice):
        self.game.answer(sid, {"token": self.game.token, "choice": choice})

    def test_correct_streak_doubles_and_wrong_busts(self):
        self.game.pot = {"a": 8, "b": 16, "c": 0}
        self.answer("a", 1)  # shuffled correct
        self.answer("b", 0)
        self.answer("c", 1)
        self.assertEqual(self.game.pot, {"a": 16, "b": 0, "c": 1})
        self.assertEqual(self.game.phase, "reveal")

    def test_secret_bank_decisions_until_everyone_has_committed(self):
        self.game.phase = "decision"
        self.game.pot = {"a": 8, "b": 4, "c": 2}
        self.vote("a", "bank")
        self.vote("b", "risk")
        self.assertEqual(self.game.phase, "decision")
        self.assertEqual(self.game.bank["a"], 0)
        self.assertFalse(any(player["decision"] is not None for player in self.game.snapshot("c")["players"]))
        self.assertTrue(all(player["decision"] is None for player in self.game.snapshot("tv")["players"]))
        self.vote("c", "risk")
        self.assertEqual(self.game.phase, "decision_reveal")
        self.assertEqual(self.game.bank["a"], 8)
        self.assertEqual(self.game.pot["a"], 0)
        self.assertEqual([p["decision"] for p in self.game.snapshot("tv")["players"]], ["bank", "risk", "risk"])

    def test_no_upcoming_question_visible_before_decision_reveal(self):
        self.game.phase = "reveal"
        self.game.next_decision()
        self.assertEqual(self.game.phase, "decision_reveal")  # All zero pots auto-risk
        self.assertNotIn("question", self.game.snapshot("a"))

    def test_stale_or_duplicate_submission_ignored(self):
        self.game.phase = "decision"
        self.game.pot = {"a": 2, "b": 2, "c": 2}
        self.game.choose("a", {"token": "bad", "choice": "bank"})
        self.vote("a", "bank")
        self.vote("a", "risk")
        self.assertEqual(self.game.decisions["a"], "bank")

    def test_final_question_correct_auto_banks_and_wrong_busts(self):
        self.game.number = 10
        self.game.pot = {"a": 16, "b": 8, "c": 0}
        self.answer("a", 1)
        self.answer("b", 0)
        self.answer("c", 1)
        self.assertEqual(self.game.bank, {"a": 32, "b": 0, "c": 1})
        self.assertEqual(self.game.pot, {"a": 0, "b": 0, "c": 0})
        self.assertTrue(self.game.results["a"]["automatic_bank"])
        self.game.finish_round()
        self.assertEqual(self.game.phase, "summary")
        self.assertEqual(self.host.players["a"]["round_score"], 32)

    def test_reconnecting_player_keeps_bank_pot_and_choice(self):
        self.game.bank["a"] = 24
        self.game.pot["a"] = 8
        self.game.decisions["a"] = "risk"
        self.game.migrate("a", "new-a")
        self.assertEqual(self.game.bank["new-a"], 24)
        self.assertEqual(self.game.pot["new-a"], 8)
        self.assertEqual(self.game.decisions["new-a"], "risk")
        self.assertNotIn("a", self.game.bank)

    def test_timer_enters_app_context(self):
        active = {"context": False}

        @contextmanager
        def context():
            active["context"] = True
            try:
                yield
            finally:
                active["context"] = False

        self.host.app.app_context = context
        seen = []
        self.game.phase = "reveal"
        self.game.delay(1, lambda: seen.append(active["context"]))
        self.socket.tasks.pop()()
        self.assertEqual(seen, [True])
        self.assertFalse(active["context"])


if __name__ == "__main__":
    unittest.main()
