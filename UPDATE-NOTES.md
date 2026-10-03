# Multiplayer recovery and scoring fixes — 3 October 2026

This is the full updated game, including the original question data, images and music. It is based on GitHub main commit `a0b95e95f1fbe781f011cd0aba105480640c8884`.

## Run this version

1. Stop the currently running game server.
2. Extract this ZIP into a separate folder so you can keep your previous working copy.
3. Use your existing Python environment with the game's dependencies. If setting up a new environment, run `python -m pip install -r requirements.txt` from the extracted game folder.
4. Run `python app.py` from that folder.
5. Open `/main` on the shared screen and the root address on the phones, using the server address printed in the console. Refresh existing browser pages to load the updated code.

Use the complete folder: this update changes templates and adds `static/js/main_screen_sequences.js`, so replacing only `app.py` is not sufficient.

## Behaviour changes

- Main-screen reconnections restore the latest question, results or final scores, including submitted-answer indicators. Interrupted introduction/explainer audio is skipped on reconnect so play can continue; final-score reconnection enables Play Again.
- Audio errors, rejected playback and stalled clips cannot leave an introduction or Play Again permanently blocked. Sequence tokens reject stale or duplicate completion messages; cancelled sequences cannot fire old callbacks into a later round.
- During Averagers team selection, reconnecting players refresh the picker's available choices. Invalid stale choices refresh the list instead of leaving the phone stuck.
- New players can join in the lobby. Once play starts, existing players can reconnect but new arrivals wait until the next lobby. This avoids unassigned late players and changes to scoring halfway through a game.
- Lobby disconnects free spaces immediately. Mid-game disconnects preserve identity and scores for reconnecting players. Play Again removes offline players and resets scores while retaining connected players.
- Answered indicators use stable unique IDs rather than names, including spaces, punctuation and duplicate names.
- Averagers compares exact team averages. The result screen shows up to two decimal places, without changing scoring precision. Genuine equal distances still share the win.

The development configuration is unchanged: only Averagers is enabled and the existing question-count settings are retained.

## Validation

- 129 Python regression checks passed, including all ten round types with 2–8 players, complete mixed games and replay, correct/incorrect scoring, reconnects and cleanup.
- 18 JavaScript checks passed for sequence timing, errors, timeouts, cancellation and restored screens.
- The suite checks HTTP routes, bundled assets, template compilation and inline JavaScript syntax.

See `tests/README.md` for test commands. Automated tests use simulated players, shortened presentation waits, and simulated audio. Actual phone/TV layout, Wi-Fi interruptions and browser playback should still be checked on your usual devices. Server-restart persistence is not added; recovery assumes the same Python game process is still running.

## GitHub workflow

The fixes are prepared on `fix/multiplayer-recovery-and-scoring` for review and real-device testing before merging into `main`. The development configuration remains unchanged.
