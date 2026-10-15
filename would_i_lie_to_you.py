"""Would I Lie to You? — server-authoritative bluffing round.

Uses the existing Flask/Socket.IO host, score helper, HTML update helper and
round summary. No timers are applied to player decisions.
"""
import json
import random
import re
import unicodedata
import uuid
from pathlib import Path


def normalise(value):
    value = unicodedata.normalize("NFKC", str(value)).casefold().strip()
    value = re.sub(r"[^\w\s]", " ", value)
    return " ".join(value.split())


class WouldILikeToYou:
    key = "would_i_lie_to_you"

    def __init__(self, host):
        self.h = host
        self.phase = "idle"
        self.token = None
        self.questions = []
        self.question_index = -1
        self.question = None
        self.bluffs = {}
        self.votes = {}
        self.options = []
        self.revealed = []
        self.awarded = {}
        self.reveal_index = 0
        self.run_id = None
        self.error = ""

    def live(self):
        return self.h.game_state == self.key + "_ongoing" and self.phase not in ("idle", "done")

    def setup(self):
        self.run_id = uuid.uuid4().hex
        self.token = uuid.uuid4().hex
        self.phase = "bluff"
        self.bluffs, self.votes, self.options, self.revealed, self.awarded = {}, {}, [], [], {}
        self.question_index = -1
        try:
            data = json.loads(Path(self.h.__file__).with_name("would_i_lie_to_you_questions.json").read_text(encoding="utf-8"))
            bank = data["questions"]
            if not isinstance(bank, list) or not bank:
                raise ValueError("The question bank is empty")
            for q in bank:
                if not q.get("question") or not q.get("answer") or not isinstance(q.get("house_bluffs"), list):
                    raise ValueError("Each question needs a prompt, answer and house bluffs")
            count = min(int(data.get("questions_per_round", 5)), len(bank))
            self.questions = random.sample(bank, count)
            self.error = ""
        except (OSError, ValueError, KeyError, TypeError) as exc:
            self.error = str(exc)
            self.questions = []
            self.phase = "error"
        for player in self.h.players.values():
            player["round_score"] = 0
        self.h.game_state = self.key + "_ongoing"
        if self.phase == "error":
            self.broadcast()
        else:
            self.next_question()

    def next_question(self):
        if not self.live():
            return
        self.question_index += 1
        if self.question_index >= len(self.questions):
            self.finish_round()
            return
        self.question = self.questions[self.question_index]
        self.phase = "bluff"
        self.token = uuid.uuid4().hex
        self.bluffs, self.votes, self.options, self.revealed = {}, {}, [], []
        self.reveal_index = 0
        self.broadcast()

    def is_truth(self, text):
        allowed = [self.question["answer"]] + self.question.get("accepted_answers", [])
        return normalise(text) in {normalise(answer) for answer in allowed}

    def submit_bluff(self, sid, data):
        if not self.live() or self.phase != "bluff" or sid not in self.h.players:
            return
        if not isinstance(data, dict) or data.get("token") != self.token or sid in self.bluffs:
            return
        value = str(data.get("text", "")).strip()
        if not 1 <= len(value) <= 80 or not normalise(value):
            self.h.socketio.emit("wilty_error", {"message": "Enter an answer of 1 to 80 characters."}, room=sid)
            return
        if self.is_truth(value):
            self.h.socketio.emit("wilty_error", {"message": "You've found the correct answer! Enter a different lie; remember the truth for voting.", "truth": True}, room=sid)
            return
        self.bluffs[sid] = value
        if set(self.h.players).issubset(self.bluffs):
            self.start_voting()
        else:
            # Don't wipe other players' half-written bluff inputs on every submission.
            self.send(self.h.main_screen_sid)
            self.send(sid)

    def start_voting(self):
        if self.phase != "bluff":
            return
        unique = {}
        for sid, text in self.bluffs.items():
            key = normalise(text)
            if key not in unique:
                unique[key] = {"id": uuid.uuid4().hex, "text": text, "authors": [], "truth": False, "house": False}
            unique[key]["authors"].append(sid)
        options = list(unique.values())
        occupied = set(unique) | {normalise(self.question["answer"])}
        for text in self.question.get("house_bluffs", []):
            if len(options) >= max(3, len(self.h.players)):
                break
            key = normalise(text)
            if key not in occupied and not self.is_truth(text):
                options.append({"id": uuid.uuid4().hex, "text": text, "authors": [], "truth": False, "house": True})
                occupied.add(key)
        options.append({"id": uuid.uuid4().hex, "text": self.question["answer"], "authors": [], "truth": True, "house": False})
        random.shuffle(options)
        self.options = options
        self.phase = "vote"
        self.token = uuid.uuid4().hex
        self.broadcast()

    def submit_vote(self, sid, data):
        if not self.live() or self.phase != "vote" or sid not in self.h.players or sid in self.votes:
            return
        if not isinstance(data, dict) or data.get("token") != self.token:
            return
        option = next((o for o in self.options if o["id"] == data.get("option_id")), None)
        if option is None or sid in option["authors"]:
            self.h.socketio.emit("wilty_error", {"message": "Choose an answer other than your own bluff."}, room=sid)
            return
        self.votes[sid] = option["id"]
        if set(self.h.players).issubset(self.votes):
            self.begin_reveal()
        else:
            self.broadcast()

    def begin_reveal(self):
        if self.phase != "vote":
            return
        for sid, option_id in self.votes.items():
            option = next(o for o in self.options if o["id"] == option_id)
            if option["truth"]:
                self.h.players[sid]["round_score"] += 2
            else:
                for author in option["authors"]:
                    self.h.players[author]["round_score"] += 1
        # Reveal false answers first, including house lies, and truth last.
        self.revealed = [o for o in self.options if not o["truth"]] + [o for o in self.options if o["truth"]]
        self.phase = "reveal"
        self.reveal_index = 0
        self.broadcast()
        self.delay("lie_reveal", self.next_reveal)

    def next_reveal(self):
        if not self.live() or self.phase != "reveal":
            return
        self.reveal_index += 1
        if self.reveal_index >= len(self.revealed):
            self.phase = "scores"
            self.broadcast()
            self.delay("question_scores", self.next_question)
        else:
            self.broadcast()
            key = "truth_reveal" if self.revealed[self.reveal_index]["truth"] else "lie_reveal"
            self.delay(key, self.next_reveal)

    def delay(self, name, callback):
        run_id = self.run_id
        phase = self.phase
        token = self.token
        seconds = float(self.h.WILTY_TIMINGS[name])

        def task():
            self.h.socketio.sleep(max(0, seconds))
            if self.run_id == run_id and self.phase == phase and self.token == token:
                # Background timers have no Flask application context. The callback
                # may render the next question, summary, or overall game-over screen.
                with self.h.app.app_context():
                    callback()

        self.h.socketio.start_background_task(task)

    def finish_round(self):
        if not self.live():
            return
        self.phase = "summary"
        self.h.game_state = self.key + "_results"
        ranked = sorted(self.h.players, key=lambda s: (-self.h.players[s]["round_score"], self.h.players[s]["name"]))
        self.awarded = self.h.award_game_points(ranked)
        overall = sorted(({"name": p["name"], "game_score": self.h.overall_game_scores.get(sid, 0)}
                          for sid, p in self.h.players.items()), key=lambda p: -p["game_score"])
        rankings = [{"rank": i + 1, "name": self.h.players[sid]["name"],
                     "round_score": self.h.players[sid]["round_score"],
                     "points_awarded": self.awarded.get(sid, 0)} for i, sid in enumerate(ranked)]
        self.h.emit_game_state_update()
        self.h.update_main_screen_html("#results-area", "_round_summary.html",
            {"round_type": "Would I Lie to You?", "rankings": rankings, "overall_scores": overall})
        self.broadcast_players()
        self.delay("round_summary", self.end)

    def end(self):
        if self.phase != "summary":
            return
        self.phase = "done"
        self.h.socketio.emit("wilty_close", room=self.h.PLAYERS_ROOM)
        self.h.start_next_game_round()

    def reset(self):
        self.run_id = None
        self.phase = "idle"
        self.h.socketio.emit("wilty_close", room=self.h.PLAYERS_ROOM)

    def migrate(self, old, new):
        for mapping in (self.bluffs, self.votes, self.awarded):
            if old in mapping:
                mapping[new] = mapping.pop(old)
        for option in self.options:
            option["authors"] = [new if sid == old else sid for sid in option["authors"]]
        for option in self.revealed:
            option["authors"] = [new if sid == old else sid for sid in option["authors"]]

    def snapshot(self, sid=None):
        d = {"phase": self.phase, "token": self.token, "number": self.question_index + 1,
             "total": len(self.questions), "error": self.error}
        if self.phase == "error":
            return d
        if self.question:
            d["question"] = self.question["question"]
        d["players"] = [{"name": p["name"], "score": p["round_score"],
                         "answered": player_sid in (self.bluffs if self.phase == "bluff" else self.votes)}
                        for player_sid, p in self.h.players.items()]
        d["submitted"] = sid in (self.bluffs if self.phase == "bluff" else self.votes)
        if self.phase == "vote":
            d["options"] = [{"id": o["id"], "text": o["text"], "own": sid in o["authors"]}
                            for o in self.options]
        if self.phase == "reveal":
            o = self.revealed[self.reveal_index]
            d["reveal"] = {"text": o["text"], "truth": o["truth"], "house": o["house"],
                           "authors": [self.h.players[a]["name"] for a in o["authors"] if a in self.h.players],
                           "voters": [self.h.players[v]["name"] for v, choice in self.votes.items()
                                      if choice == o["id"] and v in self.h.players],
                           "points": len([v for v in self.votes if self.votes[v] == o["id"]])}
            if o["truth"]:
                d["explanation"] = self.question.get("explanation", "")
        return d

    def send(self, sid):
        if not sid or self.phase in ("idle", "done", "summary"):
            return
        d = self.snapshot(sid)
        with self.h.app.app_context():
            if sid == self.h.main_screen_sid:
                self.h.update_main_screen_html("#round-content-area", "_wilty_screen.html", {"d": d, "is_main": True})
            elif sid in self.h.players:
                self.h.socketio.emit("wilty_player_prompt", {
                    "html": self.h.render_template("_wilty_screen.html", d=d, is_main=False)}, room=sid)

    def broadcast_players(self):
        for sid in self.h.players:
            if self.phase == "summary":
                self.h.socketio.emit("results_on_main_screen", room=sid)
            else:
                self.send(sid)

    def broadcast(self):
        self.send(self.h.main_screen_sid)
        self.broadcast_players()
