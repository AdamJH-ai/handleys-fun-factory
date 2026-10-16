"""Last One Standing: three-life, escalating multiple-choice survival round.

Designed for the existing Handley's Fun Factory Flask/Socket.IO host.
"""
import json
import random
import uuid
from pathlib import Path


class LastOneStanding:
    key = "last_one_standing"

    def __init__(self, host):
        self.h = host
        self.phase = "idle"
        self.run_id = None
        self.token = None
        self.questions = {}
        self.used = set()
        self.question = None
        self.number = 0
        self.lives = {}
        self.eliminated_on = {}
        self.answers = {}
        self.answer_order = []
        self.outcomes = {}
        self.error = ""
        self.awarded = {}
        self.start_lives = 3
        self.step = 2
        self.names = {1: "Easy", 2: "Normal", 3: "Tricky", 4: "Hard", 5: "Brutal"}

    def live(self):
        return self.h.game_state == self.key + "_ongoing" and self.phase in ("question", "reveal", "error")

    def active(self):
        return [sid for sid, value in self.lives.items() if value > 0 and sid in self.h.players]

    def difficulty(self):
        return min(5, 1 + (self.number - 1) // self.step)

    def setup(self):
        self.run_id = uuid.uuid4().hex
        self.token = uuid.uuid4().hex
        self.phase = "question"
        self.number = 0
        self.question = None
        self.answers, self.outcomes, self.awarded = {}, {}, {}
        self.used = set()
        self.error = ""
        self.questions = {n: [] for n in range(1, 6)}
        try:
            doc = json.loads(Path(self.h.__file__).with_name("last_one_standing_questions.json").read_text(encoding="utf-8"))
            self.start_lives = int(doc.get("starting_lives", 3))
            self.step = int(doc.get("questions_per_difficulty_step", 2))
            if self.start_lives != 3 or self.step != 2:
                raise ValueError("Last One Standing requires three lives and two questions per difficulty.")
            if doc.get("round") != self.key:
                raise ValueError("Question bank has the wrong round identifier.")
            self.names = {int(k): str(v) for k, v in doc.get("difficulty_names", {}).items()}
            for q in doc["questions"]:
                level = q.get("difficulty")
                options = q.get("options")
                correct = q.get("correct_option")
                if type(level) is not int or level not in self.questions:
                    raise ValueError("Question has invalid difficulty.")
                if not isinstance(options, list) or len(options) != 4 or len(set(options)) != 4:
                    raise ValueError("Every question must have four different options.")
                if type(correct) is not int or correct not in range(4) or options[correct] != q.get("correct_answer"):
                    raise ValueError("Question correct answer does not match its option index.")
                if not q.get("id") or not q.get("question"):
                    raise ValueError("Question ID and text are required.")
                self.questions[level].append(q)
            ids = [q["id"] for group in self.questions.values() for q in group]
            if len(ids) != len(set(ids)) or any(not self.questions[n] for n in range(1, 6)):
                raise ValueError("Question IDs must be unique and every difficulty needs questions.")
            if not self.h.players:
                raise ValueError("No players are registered.")
        except (OSError, KeyError, TypeError, ValueError) as exc:
            self.phase = "error"
            self.error = str(exc)
        self.lives = {sid: self.start_lives for sid in self.h.players}
        self.eliminated_on = {}
        for p in self.h.players.values():
            p["round_score"] = 0
        self.h.game_state = self.key + "_ongoing"
        self.h.emit_game_state_update()
        if self.phase == "error":
            self.broadcast()
        elif len(self.active()) <= 1:
            self.finish_round()
        else:
            self.next_question()

    def next_question(self):
        if not self.live() or self.phase == "error":
            return
        if len(self.active()) <= 1:
            self.finish_round()
            return
        self.number += 1
        level = self.difficulty()
        available = [q for q in self.questions[level] if q["id"] not in self.used]
        if not available and level == 5:
            # An exceptional game can continue past the entire Brutal bank.
            # Start a fresh deck only after every Brutal question has appeared;
            # avoid immediately repeating the most recent question.
            last_id = self.question["id"] if self.question else None
            available = [q for q in self.questions[5] if q["id"] != last_id]
            self.used.difference_update(q["id"] for q in self.questions[5])
        if not available:
            self.error = "No unused questions remain at difficulty " + self.names.get(level, str(level)) + ". Add more to the JSON bank."
            self.phase = "error"
            self.broadcast()
            return
        self.question = random.choice(available)
        self.used.add(self.question["id"])
        # Randomise option order independently of the bank's stored ordering.
        self.answer_order = list(range(4))
        random.shuffle(self.answer_order)
        self.phase = "question"
        self.token = uuid.uuid4().hex
        self.answers = {}
        self.outcomes = {}
        self.broadcast()

    def answer(self, sid, data):
        if not self.live() or self.phase != "question" or sid not in self.active():
            return
        if not isinstance(data, dict) or data.get("token") != self.token or sid in self.answers:
            return
        choice = data.get("choice")
        if type(choice) is not int or choice not in range(4):
            return
        self.answers[sid] = self.answer_order[choice]
        if set(self.active()).issubset(self.answers):
            self.reveal()
        else:
            # Preserve other players' pending taps; update the TV and the submitter only.
            self.send(self.h.main_screen_sid)
            self.send(sid)

    def reveal(self):
        if self.phase != "question" or not set(self.active()).issubset(self.answers):
            return
        self.outcomes = {}
        for sid in self.active():
            was_correct = self.answers[sid] == self.question["correct_option"]
            if not was_correct:
                self.lives[sid] -= 1
                if self.lives[sid] == 0:
                    self.eliminated_on[sid] = self.number
            self.outcomes[sid] = was_correct
        self.phase = "reveal"
        self.broadcast()
        self.delay(self.h.LOS_REVEAL_SECONDS, self.after_reveal)

    def after_reveal(self):
        if not self.live() or self.phase != "reveal":
            return
        if len(self.active()) <= 1:
            self.finish_round()
        else:
            self.next_question()

    def delay(self, seconds, callback):
        run_id, phase, token = self.run_id, self.phase, self.token

        def run():
            self.h.socketio.sleep(max(0, float(seconds)))
            if self.run_id == run_id and self.phase == phase and self.token == token:
                # The timer runs outside Socket.IO's request context.
                with self.h.app.app_context():
                    callback()

        self.h.socketio.start_background_task(run)

    def finish_round(self):
        if self.phase in ("summary", "done"):
            return
        if self.phase == "error":
            return
        self.phase = "summary"
        self.h.game_state = self.key + "_results"
        # Players eliminated together get the same raw round score. The sole
        # survivor ranks above players eliminated on the final question.
        for sid, p in self.h.players.items():
            p["round_score"] = self.eliminated_on.get(sid, self.number + 1)
        ranked = sorted(self.h.players, key=lambda sid: (-self.h.players[sid]["round_score"], self.h.players[sid]["name"]))
        self.awarded = self.h.award_game_points(ranked)
        overall = sorted(({"name": p["name"], "game_score": self.h.overall_game_scores.get(sid, 0)}
                          for sid, p in self.h.players.items()), key=lambda v: -v["game_score"])
        rankings = [{"rank": i + 1, "name": self.h.players[sid]["name"],
                     "round_score": self.h.players[sid]["round_score"],
                     "points_awarded": self.awarded.get(sid, 0)} for i, sid in enumerate(ranked)]
        self.h.emit_game_state_update()
        self.h.update_main_screen_html("#results-area", "_round_summary.html",
            {"round_type": "Last One Standing", "rankings": rankings, "overall_scores": overall})
        self.h.socketio.emit("results_on_main_screen", room=self.h.PLAYERS_ROOM)
        self.delay(self.h.LOS_SUMMARY_SECONDS, self.end)

    def end(self):
        if self.phase != "summary":
            return
        self.phase = "done"
        self.h.socketio.emit("los_close", room=self.h.PLAYERS_ROOM)
        self.h.start_next_game_round()

    def reset(self):
        self.run_id = None
        self.phase = "idle"
        self.h.socketio.emit("los_close", room=self.h.PLAYERS_ROOM)

    def migrate(self, old, new):
        for mapping in (self.lives, self.eliminated_on, self.answers, self.outcomes, self.awarded):
            if old in mapping:
                mapping[new] = mapping.pop(old)

    def snapshot(self, sid=None):
        d = {"phase": self.phase, "token": self.token, "number": self.number,
             "difficulty": self.difficulty() if self.number else 1,
             "difficulty_name": self.names.get(self.difficulty(), "") if self.number else "Easy",
             "error": self.error}
        if self.question:
            d["question"] = self.question["question"]
            d["options"] = [{"index": i, "text": self.question["options"][original],
                             "correct": original == self.question["correct_option"]}
                            for i, original in enumerate(self.answer_order)]
        d["players"] = [{"name": player["name"], "lives": self.lives.get(player_sid, 0),
                         "answered": player_sid in self.answers, "out": self.lives.get(player_sid, 0) <= 0,
                         "just_out": self.eliminated_on.get(player_sid) == self.number and self.phase == "reveal",
                         "result": self.outcomes.get(player_sid)}
                        for player_sid, player in self.h.players.items()]
        d["own_lives"] = self.lives.get(sid, 0)
        d["submitted"] = sid in self.answers
        if self.phase == "reveal" and self.question:
            d["answer"] = self.question["correct_answer"]
            d["explanation"] = self.question.get("explanation", "")
        if self.phase == "question":
            for o in d.get("options", []):
                o.pop("correct", None)  # Never send correct answer to clients before reveal.
        return d

    def send(self, sid):
        if not sid or self.phase in ("idle", "done", "summary"):
            return
        d = self.snapshot(sid)
        with self.h.app.app_context():
            if sid == self.h.main_screen_sid:
                self.h.update_main_screen_html("#round-content-area", "_los_screen.html", {"d": d, "is_main": True})
            elif sid in self.h.players:
                self.h.socketio.emit("los_player_prompt", {
                    "html": self.h.render_template("_los_screen.html", d=d, is_main=False)}, room=sid)

    def broadcast(self):
        self.send(self.h.main_screen_sid)
        for sid in self.h.players:
            self.send(sid)
