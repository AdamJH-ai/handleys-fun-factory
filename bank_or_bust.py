"""Bank or Bust — ten-question, six-choice double-or-nothing quiz.

All bank/risk decisions are private until every player has committed.
"""
import json
import random
import uuid
from pathlib import Path


class BankOrBust:
    key = "bank_or_bust"

    def __init__(self, host):
        self.h = host
        self.phase = "idle"
        self.run_id = None
        self.token = ""
        self.bank = {}
        self.pot = {}
        self.decisions = {}
        self.answers = {}
        self.results = {}
        self.question = None
        self.answer_order = []
        self.questions = []
        self.number = 0
        self.count = 10
        self.awarded = {}
        self.error = ""

    def live(self):
        return self.h.game_state == self.key + "_ongoing" and self.phase in (
            "decision", "decision_reveal", "question", "reveal", "error")

    def setup(self):
        self.run_id = uuid.uuid4().hex
        self.token = uuid.uuid4().hex
        self.phase = "question"
        self.bank, self.pot, self.decisions, self.answers, self.results = {}, {}, {}, {}, {}
        self.awarded = {}
        self.question = None
        self.answer_order = []
        self.number = 0
        self.error = ""
        try:
            document = json.loads(Path(self.h.__file__).with_name("bank_or_bust_questions.json").read_text(encoding="utf-8"))
            if document.get("round") != self.key:
                raise ValueError("Incorrect Bank or Bust question bank.")
            self.count = self.h.BOB_QUESTIONS_PER_ROUND
            if type(self.count) is not int or self.count < 1:
                raise ValueError("BOB_QUESTIONS_PER_ROUND must be positive.")
            bank = document["questions"]
            ids = []
            for q in bank:
                opts = q.get("options")
                idx = q.get("correct_option")
                if not isinstance(q.get("id"), str) or not q.get("question"):
                    raise ValueError("Questions need an ID and question text.")
                if not isinstance(opts, list) or len(opts) != 6 or any(not isinstance(x, str) or not x.strip() for x in opts) or len(set(opts)) != 6:
                    raise ValueError("Each Bank or Bust question needs six different answer options.")
                if type(idx) is not int or idx not in range(6) or opts[idx] != q.get("correct_answer"):
                    raise ValueError("Invalid correct answer in question " + str(q.get("id")))
                ids.append(q["id"])
            if len(set(ids)) != len(ids) or len(bank) < self.count:
                raise ValueError("Not enough distinct questions for Bank or Bust.")
            if len(self.h.players) < 1:
                raise ValueError("No players registered.")
            # Pick question IDs for this game; the next question stays hidden until
            # the entire preceding Bank/Risk decision phase has finished.
            self.questions = random.sample(bank, self.count)
        except (ValueError, TypeError, KeyError, OSError) as exc:
            self.phase = "error"
            self.error = str(exc)
        self.bank = {sid: 0 for sid in self.h.players}
        self.pot = {sid: 0 for sid in self.h.players}
        for player in self.h.players.values():
            player["round_score"] = 0
        self.h.game_state = self.key + "_ongoing"
        self.h.emit_game_state_update()
        if self.phase == "error":
            self.broadcast()
        else:
            self.show_question()  # Q1 has no Bank/Risk decision.

    def show_question(self):
        if not self.live() or self.phase == "error":
            return
        self.number += 1
        self.question = self.questions[self.number - 1]
        self.answer_order = list(range(6))
        random.shuffle(self.answer_order)
        self.answers = {}
        self.results = {}
        self.decisions = {}
        self.phase = "question"
        self.token = uuid.uuid4().hex
        self.broadcast()

    def choose(self, sid, data):
        if not self.live() or self.phase != "decision" or sid not in self.bank:
            return
        if not isinstance(data, dict) or data.get("token") != self.token or sid in self.decisions:
            return
        choice = data.get("choice")
        if choice not in ("bank", "risk"):
            return
        # A zero pot cannot be banked.
        if self.pot[sid] == 0:
            choice = "risk"
        self.decisions[sid] = choice
        if len(self.decisions) == len(self.bank):
            self.reveal_decisions()
        else:
            # Update only the submitting player's controller. No decision values
            # reach any player or the TV until ALL choices are locked in.
            self.send(sid)
            self.send(self.h.main_screen_sid)

    def reveal_decisions(self):
        if self.phase != "decision" or len(self.decisions) != len(self.bank):
            return
        for sid, choice in self.decisions.items():
            if choice == "bank":
                self.bank[sid] += self.pot[sid]
                self.pot[sid] = 0
        self.phase = "decision_reveal"
        self.broadcast()
        self.delay(self.h.BOB_DECISION_REVEAL_SECONDS, self.show_question)

    def answer(self, sid, data):
        if not self.live() or self.phase != "question" or sid not in self.bank:
            return
        if not isinstance(data, dict) or data.get("token") != self.token or sid in self.answers:
            return
        choice = data.get("choice")
        if type(choice) is not int or choice not in range(6):
            return
        self.answers[sid] = self.answer_order[choice]
        if len(self.answers) == len(self.bank):
            self.reveal_answers()
        else:
            self.send(sid)
            self.send(self.h.main_screen_sid)

    def reveal_answers(self):
        if self.phase != "question" or len(self.answers) != len(self.bank):
            return
        final_question = self.number == self.count
        for sid, answer in self.answers.items():
            correct = answer == self.question["correct_option"]
            old_pot = self.pot[sid]
            if correct:
                self.pot[sid] = old_pot * 2 if old_pot else 1
            else:
                self.pot[sid] = 0
            if final_question and correct:
                self.bank[sid] += self.pot[sid]
                self.pot[sid] = 0
            self.results[sid] = {
                "correct": correct, "old_pot": old_pot, "new_pot": self.pot[sid],
                "banked": self.bank[sid], "automatic_bank": final_question and correct
            }
        self.phase = "reveal"
        self.broadcast()
        self.delay(self.h.BOB_ANSWER_REVEAL_SECONDS,
                   self.finish_round if final_question else self.next_decision)

    def next_decision(self):
        if not self.live() or self.phase != "reveal":
            return
        self.phase = "decision"
        self.token = uuid.uuid4().hex
        self.question = None  # Never disclose the upcoming question.
        self.answer_order = []
        self.decisions = {sid: "risk" for sid in self.bank if self.pot[sid] == 0}
        if len(self.decisions) == len(self.bank):
            self.reveal_decisions()
        else:
            self.broadcast()

    def delay(self, seconds, callback):
        run_id, phase, token = self.run_id, self.phase, self.token

        def run():
            self.h.socketio.sleep(max(0, float(seconds)))
            if self.run_id == run_id and self.phase == phase and self.token == token:
                with self.h.app.app_context():
                    callback()

        self.h.socketio.start_background_task(run)

    def finish_round(self):
        if self.phase != "reveal":
            return
        self.phase = "summary"
        self.h.game_state = self.key + "_results"
        for sid, player in self.h.players.items():
            player["round_score"] = self.bank.get(sid, 0)
        ranked = sorted(self.h.players, key=lambda sid: (-self.h.players[sid]["round_score"], self.h.players[sid]["name"]))
        self.awarded = self.h.award_game_points(ranked)
        overall = sorted(({"name": player["name"], "game_score": self.h.overall_game_scores.get(sid, 0)}
                          for sid, player in self.h.players.items()), key=lambda p: -p["game_score"])
        rankings = [{"rank": i + 1, "name": self.h.players[sid]["name"],
                     "round_score": self.h.players[sid]["round_score"],
                     "points_awarded": self.awarded.get(sid, 0)} for i, sid in enumerate(ranked)]
        self.h.emit_game_state_update()
        self.h.update_main_screen_html("#results-area", "_round_summary.html",
            {"round_type": "Bank or Bust", "rankings": rankings, "overall_scores": overall})
        self.h.socketio.emit("results_on_main_screen", room=self.h.PLAYERS_ROOM)
        self.delay(self.h.BOB_SUMMARY_SECONDS, self.end)

    def end(self):
        if self.phase != "summary":
            return
        self.phase = "done"
        self.h.socketio.emit("bob_close", room=self.h.PLAYERS_ROOM)
        self.h.start_next_game_round()

    def reset(self):
        self.run_id = None
        self.phase = "idle"
        self.h.socketio.emit("bob_close", room=self.h.PLAYERS_ROOM)

    def migrate(self, old, new):
        for mapping in (self.bank, self.pot, self.decisions, self.answers, self.results, self.awarded):
            if old in mapping:
                mapping[new] = mapping.pop(old)

    def snapshot(self, sid=None):
        d = {"phase": self.phase, "token": self.token, "number": self.number,
             "total": self.count, "error": self.error,
             "submitted": sid in (self.decisions if self.phase == "decision" else self.answers),
             "own_pot": self.pot.get(sid, 0), "own_bank": self.bank.get(sid, 0)}
        # Decision data is revealed ONLY in decision_reveal.
        d["players"] = [
            {"name": player["name"], "bank": self.bank.get(pid, 0), "pot": self.pot.get(pid, 0),
             "submitted": pid in (self.decisions if self.phase == "decision" else self.answers),
             "decision": self.decisions.get(pid) if self.phase == "decision_reveal" else None,
             "result": self.results.get(pid) if self.phase == "reveal" else None}
            for pid, player in self.h.players.items()
        ]
        if self.phase in ("question", "reveal") and self.question is not None:
            d["question"] = self.question["question"]
            d["options"] = [{"index": i, "text": self.question["options"][original]}
                            for i, original in enumerate(self.answer_order)]
            if self.phase == "reveal":
                d["answer"] = self.question["correct_answer"]
                d["explanation"] = self.question.get("explanation", "")
                d["correct_index"] = self.answer_order.index(self.question["correct_option"])
                d["own_result"] = self.results.get(sid)
        return d

    def send(self, sid):
        if not sid or self.phase in ("idle", "done", "summary"):
            return
        data = self.snapshot(sid)
        with self.h.app.app_context():
            if sid == self.h.main_screen_sid:
                self.h.update_main_screen_html("#round-content-area", "_bob_screen.html", {"d": data, "is_main": True})
            elif sid in self.h.players:
                self.h.socketio.emit("bob_player_prompt", {
                    "html": self.h.render_template("_bob_screen.html", d=data, is_main=False)}, room=sid)

    def broadcast(self):
        self.send(self.h.main_screen_sid)
        for sid in self.h.players:
            self.send(sid)
