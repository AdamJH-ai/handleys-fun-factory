# Regression tests

From the repository root, with the game's dependencies installed:

```sh
python -m pip install pytest
python -m pytest -q tests/test_game.py
node tests/test_main_screen.cjs
```

The JavaScript checks use Node's built-in test runner (Node 18 or newer), with no npm packages.

Python tests exercise the real Flask/Socket.IO handlers using simulated clients. Presentation sleeps are skipped. Coverage includes all ten round types with 2–8 players, correct/incorrect scoring, complete games and replay, main-screen and player reconnections, team-selection reconnects, admission limits, lobby cleanup, stable player indicators and exact Averagers averages. Tests override round selection only in memory; the development configuration in `app.py` is unchanged.

JavaScript tests exercise the audio controller and the main-screen sequence handlers with simulated audio and timers. They cover normal sequencing, rejected playback, media errors, missing audio, stalled playback, cancelled callbacks, restored sequences and the Play Again button.

These checks do not replace a real-device playthrough. Before merging, try a game with phones and the shared screen: refresh the main screen during a question, reconnect a player during team selection, and check audio and Play Again in your usual browser. Update the Python file, templates and new `static/js/main_screen_sequences.js` together, restart the server and refresh the browser pages.
