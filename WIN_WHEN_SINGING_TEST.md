# Win When They're Singing first test

This branch adds a complete one-song test using Seven Nation Army. Run `app.py` in VS Code as usual, open `/main` on the TV computer, join on your phones, and start the game through the existing lobby. Use at least two players. Leave the TV browser in the foreground and enable its audio through the normal Start New Game interaction.

## First test settings

Near the top of `app.py`, `WWTS_FIRST_TEST = True` selects only this round and plays one song. Set it to `False` to restore the previous development selection. For normal games, add `'win_when_singing'` to `AVAILABLE_ROUND_TYPES`; `wwts_target_turns = 5` then controls the number of distinct songs required. The loader reports an error if there are not enough songs rather than silently repeating one.

The existing other-round selection and question counts are retained beneath the test override. No new runtime dependencies or launcher are required.

## What to try

1. Wait for the shared song to start. It starts fading at 10.800 seconds and reaches silence at 11.300 seconds.
2. Each player taps the large phone button when they think the first word begins. The target supplied in the JSON is 18.210 seconds. Taps are silent and private, and there is no answer deadline.
3. Once everyone has tapped, the full-volume replay starts from the beginning. Player cards highlight at their recorded times; the first-word moment is highlighted too. Playback fades out five seconds after that word. Guesses later than the replay's end remain visible in the results table.
4. Song results show early/late differences, placing points, bonus and accumulated round score. The final summary shows the existing Fun Factory points and overall totals. Both advance automatically.

The nearest three places earn 3, 2 and 1 points. Competition ranking applies: two players tied first earn 3 each and the next player earns 1. Every guess within **500 milliseconds inclusive** gets one additional bonus point. Comparisons use timestamps rounded to milliseconds. Overall points use the existing game's averaging rule for tied final standings.

## Audio and content

Edit `win_when_singing.json`. Each song needs `id`, `title`, `artist`, `audio_file`, `question_fade_start` and `first_word_time`. Times use minutes:seconds:milliseconds. Audio paths are relative to `static`, so `audio/wwts/track001.mp3` resolves to `static/audio/wwts/track001.mp3`.

Measure against the exact MP3 and keep at least five seconds after the first word. The supplied file is approximately 238.42 seconds long. Its first-word timestamp has not been independently verified by listening.

Display durations are `WWTS_SCORES_SECONDS` and `WWTS_SUMMARY_SECONDS`. The fade and reveal tail are `WWTS_FADE_SECONDS` and `WWTS_REVEAL_TAIL_SECONDS`. The round uses the existing title-card flow. No dedicated jingle or spoken explainer is supplied; these can be added through `ROUND_JINGLES` and `ROUND_EXPLAINER_INFO` once those assets exist.

## Timing and interruptions

Audio is decoded before starting. Phones and the main screen sample the server clock five times, using the least-delayed sample. The audio playback clock anchors the shared start, and phones timestamp pointer-down locally. This avoids treating message arrival time as the tap time. Real phone, TV and speaker latency still needs a home test, particularly if using Bluetooth audio.

If a participant or the TV disconnects during an attempt, the round waits for reconnection and restarts the same song. Already locked guesses and accumulated scores remain intact; players who have not submitted get the replayed attempt. During the answer replay, a TV reconnect restores that replay without scoring again. Results and final summary restore while their display timers continue. A browser reload may require the browser's audio permission to be enabled again. The game does not survive a Python server restart.

Malformed data or an audio failure produces an explanatory message rather than guessing or awarding points. For this first test, fix the issue and restart `app.py` and the game if that happens.

## Validation

- Eight automated server tests cover two/eight players, ties, bonus boundaries, invalid/duplicate/stale taps, private phone payloads, reconnects, reset cancellation, multiple songs, final point conversion and onward progression.
- Non-visual JavaScript checks exercise audio-clock anchoring, scheduled fade, replay completion and timestamp/double-tap handling. Both HTML scripts pass JavaScript syntax checks.
- Jinja renders through the actual Socket.IO integration tests.
- Chromium launch was blocked by this execution environment. TV/phone visual layout, audible playback and physical-device timing are **not browser-verified** and need this first home test.

Developer checks (test dependencies only): `python -m pytest -q tests/test_singing.py` and `node tests/test_singing_client.cjs`.
